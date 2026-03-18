"""Entry point for ``python -m src.broker``."""

from src.broker.neural_broker import _parse_args, _make_config_from_args, NeuralBroker

import logging
import uvicorn

if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = _make_config_from_args(args)
    broker = NeuralBroker(config)
    app = broker.build_app()

    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=args.log_level.lower(),
    )
