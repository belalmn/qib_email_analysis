from typing import List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from src.extract.pst_message_extractor import PstMessageExtractor, MessageChunk
from src.transform.primary_features import PrimaryFeaturesExtractor
from src.transform.derived_features import DerivedFeaturesExtractor
from src.load.data_loader import DataLoader

class EmailMessage(BaseModel):
    message_id: str
    subject: str
    body: str
    sender: str
    recipients: List[str]
    date: datetime
    attachments: List[Dict[str, Any]]
    folder_path: str
    flag: str = "normal"
    language: str = ''
    thread_id: str = ''

class ProcessedBatch(BaseModel):
    batch_id: str
    processed_at: datetime
    messages: List[EmailMessage]

class ETLConfig(BaseModel):
    input_pst_path: str
    output_directory: str
    chunk_size: int = Field(default=100, ge=1)

def main() -> None:
    config: ETLConfig = ETLConfig.model_validate_json('config/config.json')
    extractor: PstMessageExtractor = PstMessageExtractor(config.input_pst_path, config.chunk_size)
    primary_extractor: PrimaryFeaturesExtractor = PrimaryFeaturesExtractor()
    derived_extractor: DerivedFeaturesExtractor = DerivedFeaturesExtractor()
    loader: DataLoader = DataLoader(config.output_directory)

    for chunk in extractor.extract_messages():
        processed_messages: List[EmailMessage] = []
        
        for message in chunk.messages:
            primary_features: Dict[str, Any] = primary_extractor.extract(message)
            derived_features: Dict[str, Any] = derived_extractor.extract(primary_features)
            
            full_features: Dict[str, Any] = {**primary_features, **derived_features, 'folder_path': chunk.folder_path}
            email_message: EmailMessage = EmailMessage(**full_features)
            
            processed_messages.append(email_message)
        
        processed_batch: ProcessedBatch = ProcessedBatch(
            batch_id=f"batch_{datetime.now().isoformat()}",
            processed_at=datetime.now(),
            messages=processed_messages
        )
        loader.load(processed_batch)

if __name__ == "__main__":
    main()