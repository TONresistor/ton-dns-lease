// DNS Rent - Message opcodes and constants

// ===== Standard TON/NFT opcodes =====
const OP_TRANSFER: int = 0x5fcc3d14;
const OP_OWNERSHIP_ASSIGNED: int = 0x05138d91;
const OP_EXCESSES: int = 0xd53276db;
const OP_CHANGE_DNS_RECORD: int = 0x4eb1f0f9;
const OP_GET_STATIC_DATA: int = 0x2fcb26a2;

// ===== DNS Rent Listing opcodes =====
const OP_RENT: int = 0x52454e54;            // "RENT" in ASCII
const OP_CHANGE_RECORD: int = 0x4368526b;   // proxy change record
const OP_CLAIM_BACK: int = 0x436c4261;      // claim back NFT after expiration
const OP_DELIST: int = 0x44654c73;          // owner delist (no active renter)
const OP_RENEW: int = 0x52456e77;           // renter extends rental
const OP_WITHDRAW_EXCESS: int = 0x57746864; // owner withdraws excess funds
const OP_RELIST: int = 0x52654c73;          // owner re-lists after CLOSED

// ===== DNS Rent Marketplace opcodes =====
const OP_CREATE_LISTING: int = 0x1a2b3c4d;  // create new listing (legacy)
const OP_UPDATE_CODE: int = 0x75706431;     // admin update listing code
const OP_EMERGENCY_RETURN: int = 0x456d5274; // marketplace emergency NFT return

// ===== DNS Rent Listing opcodes (owner) =====
const OP_STOP_RENEWAL: int = 0x53745270;    // owner stops renewals (préavis)

// ===== Outbound notification opcodes =====
const OP_LISTING_CREATED: int = 0x4c437264; // listing created notification
const OP_RENTAL_STARTED: int = 0x52537464;  // rental started notification
const OP_RENTAL_ENDED: int = 0x52456e64;    // rental ended notification

// ===== Listing states =====
const STATE_AWAITING_NFT: int = 0;
const STATE_LISTED: int = 1;
const STATE_RENTED: int = 2;
const STATE_CLOSED: int = 3;

// ===== Gas constants =====
const MIN_TONS_FOR_STORAGE: int = 150000000;       // 0.15 TON
const GAS_CHANGE_RECORD: int = 50000000;            // 0.05 TON for proxy record change
const GAS_TRANSFER_NFT: int = 100000000;            // 0.1 TON for NFT transfer
const GAS_NOTIFICATION: int = 10000000;              // 0.01 TON for notifications
