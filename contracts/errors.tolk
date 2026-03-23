// DNS Rent - Error Codes
// Ranges: 100-109 listing lifecycle, 110-119 rent ops, 120-129 record ops, 130-139 return ops, 140-149 marketplace ops

// Listing lifecycle (100-109)
const ERR_NOT_OWNER: int = 100;
const ERR_NFT_NOT_RECEIVED: int = 101;
const ERR_NFT_ALREADY_RECEIVED: int = 102;
const ERR_WRONG_NFT: int = 103;
const ERR_WRONG_PREV_OWNER: int = 104;
const ERR_LISTING_NOT_ACTIVE: int = 105;

// Rent operations (110-119)
const ERR_ALREADY_RENTED: int = 110;
const ERR_INSUFFICIENT_PAYMENT: int = 111;
const ERR_NOT_RENTED: int = 112;
const ERR_RENTAL_EXPIRED: int = 113;
const ERR_RENTAL_NOT_EXPIRED: int = 114;

// Record operations (120-129)
const ERR_NOT_RENTER: int = 120;

// Security (125-129)
const ERR_FORBIDDEN_OP: int = 125;

// Overflow (115)
const ERR_OVERFLOW: int = 115;

// Renewal (116)
const ERR_RENEWAL_DISABLED: int = 116;

// Balance (117)
const ERR_INSUFFICIENT_BALANCE: int = 117;

// Return operations (130-139)
const ERR_ACTIVE_RENTAL: int = 130;

// Marketplace operations (140-149)
const ERR_NOT_MARKETPLACE_OWNER: int = 140;
const ERR_INVALID_PARAMS: int = 141;
const ERR_ZERO_PRICE: int = 142;
const ERR_ZERO_DURATION: int = 143;
