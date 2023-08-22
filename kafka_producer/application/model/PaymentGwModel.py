import re
import logging
import os
from dataclasses import dataclass


@dataclass
class PaymentGwModel:
    session_id: str
    uid: str