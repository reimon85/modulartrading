"""
launcher.py — Lanzador Maestro del sistema de trading ModularTrades.

Orquesta el arranque coordinado de todos los componentes:
    1. Redis (verifica conexion / arranca docker-compose)
    2. Data Fetcher (descarga OHLCV y alimenta Redis)
    3. Strategy Engine (analiza velas en vivo y emite senales)
    4. HL Smart Executor (escucha senales y ejecuta ordenes)

Uso:
    python launcher.py                      # Todos los componentes
    python launcher.py --no-executor        # Sin executor (solo datos + estrategia)
    python launcher.py --no-strategy        # Sin estrategia (solo datos + executor)
    python launcher.py --dry-run            # Executor en modo simulado
    python launcher.py --fetcher-only       # Solo el Data Fetcher
    python launcher.py --status             # Ver estado de los servicios
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

# ── Project imports ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from src import config  # noqa: E402

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("launcher")

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable


# ═══════════════════════════════════════════════════════════════════
#  1. HEALTH CHECKS
# ═══════════════════════════════════════════════════════════════════

async def check_redis() -> bool:
    """Verifica conexion a Redis. Devuelve True si responde al PING."""
    try:
        import redis.asyncio as aioredis

        pool = aioredis.ConnectionPool.from_url(
            config.REDIS_URL, decode_responses=True, max_connections=1,
        )
        r = aioredis.Redis(connection_pool=pool)
        await r.ping()
        await r.aclose()
        return True
    except Exception:
        return False


def docker_compose_up() -> bool:
    """Intenta arrancar Redis via docker-compose."""
    compose_file = PROJECT_ROOT / "docker-compose.yml"
    if not compose_file.exists():
        logger.error("docker-compose.yml no encontrado en %s", PROJECT_ROOT)
        return False

    logger.info("Arrancando Redis via docker-compose ...")
    try:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "up", "-d"],
            check=True, capture_output=True, text=True,
            cwd=str(PROJECT_ROOT),
        )
        return True
    except FileNotFoundError:
        # Try legacy docker-compose command
        try:
            subprocess.run(
                ["docker-compose", "-f", str(compose_file), "up", "-d"],
                check=True, capture_output=True, text=True,
                cwd=str(PROJECT_ROOT),
            )
            return True
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            logger.error("docker-compose no disponible: %s", exc)
            return False
    except subprocess.CalledProcessError as exc:
        logger.error("docker-compose up fallo: %s", exc.stderr)
        return False


async def ensure_redis() -> bool:
    """Asegura que Redis esta corriendo. Intenta docker-compose si no responde."""
    if await check_redis():
        logger.info("Redis OK (ya corriendo)")
        return True

    logger.warning("Redis no responde — intentando docker-compose up ...")
    if not docker_compose_up():
        logger.error("No se pudo arrancar Redis")
        return False

    # Esperar hasta 10s a que Redis responda
    for i in range(10):
        await asyncio.sleep(1)
        if await check_redis():
            logger.info("Redis arrancado correctamente (intento %d)", i + 1)
            return True

    logger.error("Redis no respondio tras 10 segundos")
    return False


# ═══════════════════════════════════════════════════════════════════
#  2. PROCESS MANAGER
# ═══════════════════════════════════════════════════════════════════

class ProcessManager:
    """Gestiona subprocesos con shutdown coordinado."""

    def __init__(self):
        self._procs: dict[str, subprocess.Popen] = {}
        self._stopping = False

    def launch(self, name: str, cmd: list[str], env: dict | None = None) -> bool:
        """Lanza un subproceso con nombre identificativo."""
        if name in self._procs and self._procs[name].poll() is None:
            logger.warning("%s ya esta corriendo (PID %d)", name, self._procs[name].pid)
            return True

        proc_env = os.environ.copy()
        if env:
            proc_env.update(env)

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                env=proc_env,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            self._procs[name] = proc
            logger.info("[%s] Arrancado (PID %d) | cmd: %s", name, proc.pid, " ".join(cmd))
            return True
        except Exception as exc:
            logger.error("[%s] Fallo al arrancar: %s", name, exc)
            return False

    def is_alive(self, name: str) -> bool:
        proc = self._procs.get(name)
        return proc is not None and proc.poll() is None

    def status(self) -> dict[str, str]:
        result = {}
        for name, proc in self._procs.items():
            rc = proc.poll()
            if rc is None:
                result[name] = f"RUNNING (PID {proc.pid})"
            else:
                result[name] = f"STOPPED (exit code {rc})"
        return result

    def stop_all(self) -> None:
        """Detiene todos los subprocesos de forma ordenada."""
        if self._stopping:
            return
        self._stopping = True

        logger.info("Deteniendo todos los servicios ...")
        for name in reversed(list(self._procs.keys())):
            self._stop_one(name)

    def _stop_one(self, name: str) -> None:
        proc = self._procs.get(name)
        if proc is None or proc.poll() is not None:
            return

        logger.info("[%s] Enviando SIGTERM (PID %d) ...", name, proc.pid)
        proc.terminate()
        try:
            proc.wait(timeout=5)
            logger.info("[%s] Detenido limpiamente", name)
        except subprocess.TimeoutExpired:
            logger.warning("[%s] No respondio a SIGTERM — enviando SIGKILL", name)
            proc.kill()
            proc.wait(timeout=3)


# ═══════════════════════════════════════════════════════════════════
#  3. ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════

class Orchestrator:
    """
    Coordina el arranque ordenado y monitoreo de componentes.

    Orden de arranque:
        Redis → Data Fetcher → Strategy (live) → Executor
    """

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.pm = ProcessManager()
        self._running = True

    async def run(self) -> int:
        """Punto de entrada principal. Devuelve exit code."""

        self._print_banner()

        # 1. Redis
        if not await ensure_redis():
            return 1

        # 2. Data Fetcher
        if not self._launch_fetcher():
            return 1

        # Esperar a que el fetcher complete su ciclo inicial
        logger.info("Esperando al Data Fetcher (ciclo inicial) ...")
        await self._wait_for_process("fetcher", timeout=120)

        # 3. Strategy Engine (si habilitada)
        if self.args.enable_strategy:
            self._launch_strategy()
            await asyncio.sleep(2)  # Dar tiempo a conectar Redis

        # 4. Executor (si habilitado)
        if self.args.enable_executor:
            self._launch_executor()
            await asyncio.sleep(1)

        # 5. Monitor loop
        if self.args.enable_strategy or self.args.enable_executor:
            return await self._monitor_loop()

        logger.info("Solo se lanzo el Data Fetcher (one-shot) — fin.")
        return 0

    # ── Launchers ──────────────────────────────────────────────

    def _launch_fetcher(self) -> bool:
        cmd = [
            PYTHON, "-m", "src.main",
            "--symbol", config.SYMBOL,
            "--timeframe", config.TIMEFRAME,
            "--days", str(self.args.fetcher_days),
        ]
        return self.pm.launch("fetcher", cmd)

    def _launch_strategy(self) -> bool:
        cmd = [
            PYTHON, "my_strategy.py", "live",
            "--poll", str(self.args.strategy_poll),
            "--timeframe", config.TIMEFRAME,
        ]
        return self.pm.launch("strategy", cmd)

    def _launch_executor(self) -> bool:
        cmd = [PYTHON, "-m", "executor.hl_smart_executor"]
        if self.args.dry_run:
            cmd.append("--dry-run")
        return self.pm.launch("executor", cmd)

    # ── Monitor ────────────────────────────────────────────────

    async def _monitor_loop(self) -> int:
        """Monitorea procesos y reinicia los que caigan."""
        logger.info("Monitor activo — Ctrl+C para detener todo")

        restart_counts: dict[str, int] = {}
        max_restarts = 5

        while self._running:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break

            for name in list(self.pm._procs.keys()):
                if name == "fetcher":
                    continue  # fetcher es one-shot

                if not self.pm.is_alive(name):
                    count = restart_counts.get(name, 0)
                    if count >= max_restarts:
                        logger.error(
                            "[%s] Excedio max reinicios (%d) — no se reinicia mas",
                            name, max_restarts,
                        )
                        continue

                    rc = self.pm._procs[name].returncode
                    logger.warning("[%s] Caido (exit=%s) — reiniciando (%d/%d) ...",
                                   name, rc, count + 1, max_restarts)

                    restart_counts[name] = count + 1

                    if name == "strategy":
                        self._launch_strategy()
                    elif name == "executor":
                        self._launch_executor()

                    await asyncio.sleep(2)

        self.pm.stop_all()
        return 0

    async def _wait_for_process(self, name: str, timeout: int = 60) -> bool:
        """Espera a que un proceso termine (para one-shots como fetcher)."""
        proc = self.pm._procs.get(name)
        if proc is None:
            return False

        start = time.time()
        while time.time() - start < timeout:
            if proc.poll() is not None:
                rc = proc.returncode
                if rc == 0:
                    logger.info("[%s] Completado OK", name)
                    return True
                else:
                    logger.error("[%s] Fallo con exit code %d", name, rc)
                    return False
            await asyncio.sleep(1)

        logger.warning("[%s] Timeout tras %ds", name, timeout)
        return False

    # ── Banner ─────────────────────────────────────────────────

    def _print_banner(self) -> None:
        components = []
        components.append("Data Fetcher")
        if self.args.enable_strategy:
            components.append("Strategy (live)")
        if self.args.enable_executor:
            mode = "DRY RUN" if self.args.dry_run else "LIVE"
            components.append(f"Executor ({mode})")

        print(f"""
================================================================
   MODULAR TRADES — MASTER LAUNCHER
================================================================
   Symbol:      {config.SYMBOL}
   Timeframe:   {config.TIMEFRAME}
   Components:  {' + '.join(components)}
   Redis:       {config.REDIS_HOST}:{config.REDIS_PORT}
================================================================
""")


# ═══════════════════════════════════════════════════════════════════
#  4. STATUS COMMAND
# ═══════════════════════════════════════════════════════════════════

async def show_status() -> None:
    """Muestra el estado de los servicios del sistema."""
    print()
    print("=" * 50)
    print("  MODULAR TRADES — STATUS")
    print("=" * 50)

    # Redis
    redis_ok = await check_redis()
    print(f"  Redis:     {'OK' if redis_ok else 'DOWN'}")

    if redis_ok:
        import redis.asyncio as aioredis

        r = aioredis.from_url(config.REDIS_URL, decode_responses=True)
        try:
            # Candles stored
            tf_key = f"ohlcv:btc_usdt:{config.TIMEFRAME}"
            candle_count = await r.zcard(tf_key)
            print(f"  Candles:   {candle_count} ({config.TIMEFRAME})")

            # Tick snapshot
            tick = await r.hgetall("ticker:btc_usdt:latest")
            if tick:
                price = tick.get("price", "n/a")
                print(f"  Last tick: ${float(price):,.2f}" if price != "n/a" else "  Last tick: n/a")
            else:
                print("  Last tick: (no data)")

            # Active positions
            active = await r.smembers("executor:active_pairs")
            if active:
                print(f"  Positions: {', '.join(active)}")
            else:
                print("  Positions: none")

            # Signal channel subscribers
            pubsub_info = await r.execute_command("PUBSUB", "NUMSUB", config.HL_SIGNAL_CHANNEL)
            subs = int(pubsub_info[1]) if len(pubsub_info) > 1 else 0
            print(f"  Signal ch: {config.HL_SIGNAL_CHANNEL} ({subs} subs)")

        finally:
            await r.aclose()

    print("=" * 50)
    print()


# ═══════════════════════════════════════════════════════════════════
#  5. ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ModularTrades — Master Launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ejemplos:
  python launcher.py                       Todo: fetcher + strategy + executor (dry-run)
  python launcher.py --dry-run             Executor en modo simulado
  python launcher.py --no-executor         Solo datos + estrategia
  python launcher.py --no-strategy         Solo datos + executor
  python launcher.py --fetcher-only        Solo descarga de datos
  python launcher.py --status              Ver estado de servicios
        """,
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Muestra estado de los servicios y sale",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Executor en modo simulado (sin ordenes reales)",
    )
    parser.add_argument(
        "--no-executor", action="store_true",
        help="No lanzar el executor",
    )
    parser.add_argument(
        "--no-strategy", action="store_true",
        help="No lanzar la estrategia",
    )
    parser.add_argument(
        "--fetcher-only", action="store_true",
        help="Solo ejecutar el Data Fetcher (one-shot)",
    )
    parser.add_argument(
        "--fetcher-days", type=int, default=7,
        help="Dias de historico a descargar (default: 7)",
    )
    parser.add_argument(
        "--strategy-poll", type=int, default=10,
        help="Intervalo de polling de la estrategia en segundos (default: 10)",
    )

    args = parser.parse_args()

    # Status mode
    if args.status:
        asyncio.run(show_status())
        return

    # Resolve component flags
    if args.fetcher_only:
        args.enable_strategy = False
        args.enable_executor = False
    else:
        args.enable_strategy = not args.no_strategy
        args.enable_executor = not args.no_executor

    # Auto dry-run if no private key
    if args.enable_executor and not config.HL_PRIVATE_KEY:
        args.dry_run = True
        logger.info("HL_PRIVATE_KEY vacio — forzando --dry-run")

    orchestrator = Orchestrator(args)

    # Graceful shutdown on signals
    def _signal_handler(sig, frame):
        logger.info("Senal recibida (%s) — deteniendo ...", sig)
        orchestrator._running = False
        orchestrator.pm.stop_all()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    exit_code = asyncio.run(orchestrator.run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
