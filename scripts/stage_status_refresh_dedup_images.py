"""ADR0157: explicitly invoked image-cache staging; no customer traffic or service deployment."""

from run_status_refresh_dedup_comparison import create_stager

if __name__ == "__main__":
    create_stager().main()
