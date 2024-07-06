from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field
from src.extract.pst_extractor import PstMessageExtractor, MessageChunk
from src.transform.primary_features import PrimaryFeaturesExtractor, PrimaryFeatures
#from src.transform.derived_features import DerivedFeaturesExtractor
#from src.load.data_loader import DataLoader
from os import path
import json
import logging

logging.basicConfig(level=logging.INFO)

class EmailMessage(BaseModel):
    identifier: str
    subject: str
    sender_name: str
    transport_headers: str
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    cc_address: Optional[str] = None
    bcc_address: Optional[str] = None
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    attachment_count: int
    body: str
    folder_name: str
    # Derived features
    # flag: str = "normal"
    # language: str = None
    # thread_id: str = None

class ProcessedBatch(BaseModel):
    batch_id: str
    processed_at: datetime
    messages: List[EmailMessage]

class ETLConfig(BaseModel):
    model_config = ConfigDict(strict=True)
    input_pst_path: str = Field(default="./data/raw/emails.pst")
    output_directory: str = Field(default="./data/processed/")
    chunk_size: int = Field(default=250, ge=1)
    
    def normalize_paths(self):
        self.input_pst_path = path.normcase(self.input_pst_path)
        self.output_directory = path.normcase(self.output_directory)
    
    @classmethod
    def from_json(cls, config_path: str) -> 'ETLConfig':
        try:
            with open(config_path, "r") as config_file:
                config_json: str = config_file.read()
                config = cls.model_validate_json(config_json)
                config.normalize_paths()
                return config
        except FileNotFoundError as e:
            logging.warning(f"Config file not found: {e}, using default config")
            config = cls()
            config.normalize_paths()
            return config

def main() -> None:
    config: ETLConfig = ETLConfig.from_json("config.json")
    extractor: PstMessageExtractor = PstMessageExtractor(config.input_pst_path, config.chunk_size)
    primary_extractor: PrimaryFeaturesExtractor = PrimaryFeaturesExtractor()
    #derived_extractor: DerivedFeaturesExtractor = DerivedFeaturesExtractor()
    #loader: DataLoader = DataLoader(config.output_directory)

    for chunk in extractor.extract_messages():
        processed_messages: List[EmailMessage] = []
        
        for message in chunk.messages:
            primary_features: PrimaryFeatures = primary_extractor.extract(message, chunk.folder_path)
            #derived_features: Dict[str, Any] = derived_extractor.extract(primary_features.dict())
            
            email_message = EmailMessage(
                **primary_features.model_dump(),
                #**derived_features
            )
            
            processed_messages.append(email_message)
        
        processed_batch: ProcessedBatch = ProcessedBatch(
            batch_id=f"batch_{datetime.now().isoformat()}",
            processed_at=datetime.now(),
            messages=processed_messages
        )
        #loader.load(processed_batch)

if __name__ == "__main__":
    main()