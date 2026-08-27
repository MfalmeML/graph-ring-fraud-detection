from enum import Enum

class NodeType(Enum):
    ACCOUNT = "Account"
    DEVICE = "Device"
    IP = "IP"
    MERCHANT = "Merchant"
    CARD = "Card"

class EdgeType(Enum):
    USED = "USED"
    SEEN_AT = "SEEN_AT"
    TRANSACTED_WITH = "TRANSACTED_WITH"
    OWNS = "OWNS"