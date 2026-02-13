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
from src.notifier import AlertLevel, TelegramNotifier  # noqa: E402

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
        self._notifier = TelegramNotifier()
        self._running = True

    async def run(self) -> int:
        """Punto de entrada principal. Devuelve exit code."""

        self._print_banner()
        await self._notifier.connect()

        # 1. Redis
        if not await ensure_redis():
            return 1

        # 2. Initial Data Sync (One-shot)
        tfs = config.TIMEFRAME
        if getattr(self.args, "dca", False):
            if "15m" not in tfs: tfs = f"{tfs},15m"
            if "1d" not in tfs: tfs = f"{tfs},1d"
            
        if not self._launch_initial_sync(tfs):
            return 1

        # Esperar a que el sync inicial complete
        logger.info("Esperando al Sync Inicial (todos los timeframes) ...")
        await self._wait_for_process("initial_sync", timeout=300)

        # 3. Continuous Fetcher (Service)
        self._launch_continuous_fetcher(tfs)

        # 4. Strategy Engine (si habilitada)
        if self.args.enable_strategy:
            self._launch_strategy()
            await asyncio.sleep(2)

        # 5. Executor (si habilitado)
        if self.args.enable_executor:
            self._launch_executor()
            await asyncio.sleep(1)

        # 6. Notify system started
        components = ["Fetcher(Continuous)"]
        if self.args.enable_strategy:
            components.append("Strategy")
        if self.args.enable_executor:
            components.append(f"Executor({getattr(self.args, 'executor_targets', [])})")
            
        await self._notifier.notify(
            AlertLevel.INFO, "System started",
            f"  Components: {', '.join(components)}",
            source="Launcher",
        )

        # 7. Monitor loop
        rc = await self._monitor_loop()
        await self._notifier.disconnect()
        return rc

    # ── Launchers ──────────────────────────────────────────────

    def _launch_initial_sync(self, timeframes: str) -> bool:
        cmd = [
            PYTHON, "-m", "src.main",
            "--symbol", config.SYMBOL,
            "--timeframe", timeframes,
            "--days", str(self.args.fetcher_days),
        ]
        return self.pm.launch("initial_sync", cmd)

    def _launch_continuous_fetcher(self, timeframes: str) -> bool:
        cmd = [
            PYTHON, "-m", "src.continuous_fetcher",
            "--symbol", config.SYMBOL,
            "--timeframes", timeframes,
        ]
        return self.pm.launch("fetcher_continuous", cmd)

    def _launch_strategy(self) -> bool:
        if getattr(self.args, "dca", False):
            cmd = [
                PYTHON, "dca_strategy.py",
                "--symbol", config.SYMBOL,
                "--poll", str(self.args.strategy_poll),
            ]
            name = "strategy_dca"
        else:
            cmd = [
                PYTHON, "my_strategy.py", "live",
                "--poll", str(self.args.strategy_poll),
                "--timeframe", config.TIMEFRAME,
            ]
            name = "strategy"
        return self.pm.launch(name, cmd)

    def _launch_executor(self) -> bool:
        """Lanza el(los) executor(es) segun flags."""
        launched = False

        targets = getattr(self.args, "executor_targets", ["hl"])
        
        for target in targets:
            cmd = [PYTHON, "-m"]
            if target == "hl": cmd.append("executor.hl_smart_executor")
            elif target == "lighter": cmd.append("executor.lighter_executor")
            elif target == "hyena": cmd.append("executor.hyena_executor")
            elif target == "extended": cmd.append("executor.extended_executor")
            
            if self.args.dry_run:
                cmd.append("--dry-run")
            
            name = f"executor_{target}"
            launched = self.pm.launch(name, cmd) or launched

        return launched

    def _relaunch_executor(self, name: str) -> bool:
        """Relanza un executor especifico por nombre."""
        target = name.replace("executor_", "")
        cmd = [PYTHON, "-m"]
        if target == "hl": cmd.append("executor.hl_smart_executor")
        elif target == "lighter": cmd.append("executor.lighter_executor")
        elif target == "hyena": cmd.append("executor.hyena_executor")
        elif target == "extended": cmd.append("executor.extended_executor")
        else: return False
        
        if self.args.dry_run:
            cmd.append("--dry-run")
        return self.pm.launch(name, cmd)

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
                if name == "initial_sync":
                    continue  # sync inicial es one-shot

                if not self.pm.is_alive(name):
                    count = restart_counts.get(name, 0)
                    if count >= max_restarts:
                        logger.error(
                            "[%s] Excedio max reinicios (%d) — no se reinicia mas",
                            name, max_restarts,
                        )
                        await self._notifier.notify(
                            AlertLevel.CRITICAL,
                            f"Process {name} exceeded max restarts ({max_restarts})",
                            source="Launcher",
                        )
                        continue

                    rc = self.pm._procs[name].returncode
                    logger.warning("[%s] Caido (exit=%s) — reiniciando (%d/%d) ...",
                                   name, rc, count + 1, max_restarts)
                    await self._notifier.notify(
                        AlertLevel.WARNING,
                        f"Process {name} crashed (exit={rc})",
                        f"  Restarting ({count + 1}/{max_restarts})",
                        source="Launcher",
                    )

                    restart_counts[name] = count + 1

                    if name == "fetcher_continuous":
                        tfs = config.TIMEFRAME
                        if getattr(self.args, "dca", False):
                            if "15m" not in tfs: tfs = f"{tfs},15m"
                            if "1d" not in tfs: tfs = f"{tfs},1d"
                        self._launch_continuous_fetcher(tfs)
                    elif name.startswith("strategy"):
                        self._launch_strategy()
                    elif name.startswith("executor"):
                        self._relaunch_executor(name)

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
                    await self._notifier.notify(
                        AlertLevel.CRITICAL, f"Process {name} failed (exit={rc})",
                        source="Launcher",
                    )
                    return False
            await asyncio.sleep(1)

        logger.warning("[%s] Timeout tras %ds", name, timeout)
        return False

    # ── Banner ─────────────────────────────────────────────────

    def _print_banner(self) -> None:
        components = ["Fetcher (Multi-TF)"]
        if self.args.enable_strategy:
            name = "DCA" if getattr(self.args, "dca", False) else "Standard"
            components.append(f"Strategy ({name})")
        if self.args.enable_executor:
            mode = "DRY RUN" if self.args.dry_run else "LIVE"
            targets = getattr(self.args, "executor_targets", ["HL"])
            components.append(f"Executors {targets} ({mode})")

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
  python launcher.py                       Todo: fetcher + strategy + HL executor
  python launcher.py --dry-run             Executor en modo simulado
  python launcher.py --lighter             Usar Lighter.xyz en vez de Hyperliquid
  python launcher.py --both-executors      Ambos executors en paralelo
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
        "--dca", action="store_true",
        help="Usar la estrategia DCA Pullback (45m) en lugar de la estandar",
    )
    parser.add_argument(
        "--fetcher-only", action="store_true",
        help="Solo ejecutar el Data Fetcher (one-shot)",
    )
    parser.add_argument(
        "--fetcher-days", type=int, default=80,
        help="Dias de historico a descargar (default: 80)",
    )
    parser.add_argument(
        "--strategy-poll", type=int, default=10,
        help="Intervalo de polling de la estrategia en segundos (default: 10)",
    )
    parser.add_argument(
        "--lighter", action="store_true",
        help="Usar Lighter.xyz executor",
    )
    parser.add_argument(
        "--hyena", action="store_true",
        help="Usar HyENA executor",
    )
    parser.add_argument(
        "--extended", action="store_true",
        help="Usar Extended (X10) executor",
    )
    parser.add_argument(
        "--both-executors", action="store_true",
        help="Lanzar Hyperliquid + Lighter",
    )
    parser.add_argument(
        "--all-executors", action="store_true",
        help="Lanzar TODOS los executors disponibles",
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

    # Resolve executor target
    args.executor_targets = []
    if args.all_executors:
        args.executor_targets = ["hl", "lighter", "hyena", "extended"]
    elif args.both_executors:
        args.executor_targets = ["hl", "lighter"]
    else:
        if args.lighter: args.executor_targets.append("lighter")
        if args.hyena: args.executor_targets.append("hyena")
        if args.extended: args.executor_targets.append("extended")
        if not args.executor_targets: args.executor_targets.append("hl")

    # Auto dry-run if no private keys for the selected executor(s)
    if args.enable_executor:
        all_keys_missing = True
        for target in args.executor_targets:
            if target == "hl" and config.HL_PRIVATE_KEY: all_keys_missing = False
            if target == "lighter" and config.LIGHTER_API_KEY: all_keys_missing = False
            if target == "hyena" and config.HYENA_PRIVATE_KEY: all_keys_missing = False
            if target == "extended" and config.EXTENDED_API_KEY: all_keys_missing = False
        
        if all_keys_missing and not args.dry_run:
            args.dry_run = True
            logger.info("No se detectaron API keys para los targets seleccionados — forzando --dry-run")

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
