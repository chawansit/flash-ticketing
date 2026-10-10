"""ADR0266 distinct supervised lag job executing the verified frozen observer."""
import runpy

if __name__ == "__main__":
    runpy.run_path("/app/scripts/kafka_lag_observe.py", run_name="__main__")
