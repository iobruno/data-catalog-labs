import argparse

from airbyte_datahub.config import load_config
from airbyte_datahub.emitter import emit_all


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Path to pipeline YAML config file")
    args = parser.parse_args()

    config = load_config(args.config)
    emit_all(config)


if __name__ == "__main__":
    main()
