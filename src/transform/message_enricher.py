import re
from typing import List, Optional, Union

import pypff
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.extract.message_parser import ParsedMessage

class EnrichedMessage(ParsedMessage):
    