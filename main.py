from typing import List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from src.extract.pst_extractor import PstMessageExtractor, MessageChunk
from src.transform.primary_features import PrimaryFeaturesExtractor, PrimaryFeatures
#from src.transform.derived_features import DerivedFeaturesExtractor
#from src.load.data_loader import DataLoader

class EmailMessage(BaseModel):
    identifier: str
    subject: str
    sender_name: str
    transport_headers: str
    from_address: str = None
    to_address: str = None
    cc_address: str = None
    bcc_address: str = None
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    attachment_count: int
    body: str
    folder_name: str
    # Derived features
    flag: str = "normal"
    language: str = None
    thread_id: str = None

class ProcessedBatch(BaseModel):
    batch_id: str
    processed_at: datetime
    messages: List[EmailMessage]

class ETLConfig(BaseModel):
    input_pst_path: str
    output_directory: str
    chunk_size: int = Field(default=100, ge=1)

def main() -> None:
    config: ETLConfig = ETLConfig.parse_file('config.json')
    extractor: PstMessageExtractor = PstMessageExtractor(config.input_pst_path, config.chunk_size)
    primary_extractor: PrimaryFeaturesExtractor = PrimaryFeaturesExtractor()
    derived_extractor: DerivedFeaturesExtractor = DerivedFeaturesExtractor()
    loader: DataLoader = DataLoader(config.output_directory)

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