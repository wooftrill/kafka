import re
import logging
import os
from dataclasses import dataclass


@dataclass
class PaymentGwModel:
    session_id: str
    uid: str
    order_id: str
    pg_order_id: str
    signature: str
    payment_status: int