import json
import logging
from datetime import datetime
from os import path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from src.extract.pst_extractor import (
    MessageChunk,
    PrimaryEmailFeatures,
    ProcessedBatch,
    PstMessageExtractor,
)
from src.transform.primary_features import PrimaryFeatures, PrimaryFeaturesExtractor
from src.utils.config import Config

# from src.transform.derived_features import DerivedFeaturesExtractor
# from src.load.data_loader import DataLoader

logging.basicConfig(level=logging.INFO)


def main() -> None:
    config: Config = Config.from_json("config.json")
    extractor: PstMessageExtractor = PstMessageExtractor(
        config.input_pst_path, config.chunk_size
    )
    primary_extractor: PrimaryFeaturesExtractor = PrimaryFeaturesExtractor()
    # derived_extractor: DerivedFeaturesExtractor = DerivedFeaturesExtractor()
    # loader: DataLoader = DataLoader(config.output_directory)

    for chunk in extractor.extract_messages():
        processed_messages: List[PrimaryEmailFeatures] = []

        for message in chunk.messages:
            primary_features: PrimaryFeatures = primary_extractor.extract(
                message, chunk.folder_path
            )
            # derived_features: Dict[str, Any] = derived_extractor.extract(primary_features.dict())

            email_message = PrimaryEmailFeatures(
                **primary_features.model_dump(),
                # **derived_features
            )

            processed_messages.append(email_message)

        processed_batch: ProcessedBatch = ProcessedBatch(
            batch_id=f"batch_{datetime.now().isoformat()}",
            processed_at=datetime.now(),
            messages=processed_messages,
        )
        # loader.load(processed_batch)


if __name__ == "__main__":
    main()
