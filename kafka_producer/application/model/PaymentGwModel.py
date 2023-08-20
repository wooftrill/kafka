import re
import logging
import os
from dataclasses import dataclass


@dataclass
class PaymentGwModel:
    order_id: str
    pg_order_id: str