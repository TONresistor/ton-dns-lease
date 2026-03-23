tolk 1.2

import "@stdlib/gas-payments"
import "errors.tolk"
import "messages.tolk"

// ============================================================
// Global storage variables
// ============================================================
global marketplaceAddr: address;
global nftAddress: address;
global ownerAddress: address;
global renterAddress: address?;  // null when no renter
global rentalPrice: int;
global rentalDuration: int;
global rentalEndTime: int;
global state: int;
global nftReceived: int;
global renewalAllowed: int;

// ============================================================
// Storage load / save
// ============================================================
fun loadStorage() {
    var cs: slice = contract.getData().beginParse();
    marketplaceAddr = cs.loadAddress();
    nftAddress = cs.loadAddress();
    ownerAddress = cs.loadAddress();
    state = cs.loadUint(8);
    nftReceived = cs.loadInt(1);
    // Rental fields in ref cell
    var rs: slice = cs.loadRef().beginParse();
    renterAddress = rs.loadAddressOpt();
    rentalPrice = rs.loadCoins();
    rentalDuration = rs.loadUint(32);
    rentalEndTime = rs.loadUint(32);
    renewalAllowed = rs.loadInt(1);
}

fun saveStorage() {
    var rentalRef: cell = beginCell()
        .storeAddressOpt(renterAddress)
        .storeCoins(rentalPrice)
        .storeUint(rentalDuration, 32)
        .storeUint(rentalEndTime, 32)
        .storeInt(renewalAllowed, 1)
        .endCell();
    contract.setData(
        beginCell()
            .storeAddress(marketplaceAddr)
            .storeAddress(nftAddress)
            .storeAddress(ownerAddress)
            .storeUint(state, 8)
            .storeInt(nftReceived, 1)
            .storeRef(rentalRef)
            .endCell()
    );
}

// ============================================================
// Helpers
// ============================================================
fun sendExcess(dest: address) {
    sendRawMessage(
        beginCell()
            .storeUint(0x10, 6)
            .storeAddress(dest)
            .storeCoins(0)
            .storeUint(0, 1 + 4 + 4 + 64 + 32 + 1 + 1)
            .storeUint(OP_EXCESSES, 32)
            .storeUint(0, 64)
            .endCell(),
        SEND_MODE_CARRY_ALL_BALANCE | SEND_MODE_IGNORE_ERRORS
    );
}

fun sendTons(dest: address, amount: int) {
    sendRawMessage(
        beginCell()
            .storeUint(0x10, 6)
            .storeAddress(dest)
            .storeCoins(amount)
            .storeUint(0, 1 + 4 + 4 + 64 + 32 + 1 + 1)
            .endCell(),
        SEND_MODE_IGNORE_ERRORS
    );
}

fun sendNftTransfer(queryId: int, newOwner: address, responseDest: address) {
    var body: cell = beginCell()
        .storeUint(OP_TRANSFER, 32)
        .storeUint(queryId, 64)
        .storeAddress(newOwner)
        .storeAddress(responseDest)
        .storeUint(0, 1)       // no custom_payload
        .storeCoins(0)         // forward_amount = 0
        .storeUint(0, 1)       // no forward_payload
        .endCell();
    sendRawMessage(
        beginCell()
            .storeUint(0x18, 6)
            .storeAddress(nftAddress)
            .storeCoins(GAS_TRANSFER_NFT)
            .storeUint(0, 1 + 4 + 4 + 64 + 32)   // default fields
            .storeUint(0, 1)       // no state_init
            .storeUint(1, 1)       // body in ref
            .storeRef(body)
            .endCell(),
        0
    );
}

// ============================================================
// OP_OWNERSHIP_ASSIGNED — NFT received notification
// ============================================================
fun handleOwnershipAssigned(senderAddr: address, msgBody: slice) {
    assert(senderAddr == nftAddress) throw ERR_WRONG_NFT;
    assert(state == STATE_AWAITING_NFT) throw ERR_NFT_ALREADY_RECEIVED;

    var queryId: int = msgBody.loadUint(64);
    // prev_owner not checked - NFT can arrive from owner or marketplace
    var prevOwner: address = msgBody.loadAddress();

    nftReceived = -1;  // true
    state = STATE_LISTED;
    saveStorage();

    reserveToncoinsOnBalance(MIN_TONS_FOR_STORAGE, RESERVE_MODE_AT_MOST);
    sendExcess(ownerAddress);
}

// ============================================================
// OP_RENT — Someone rents the domain
// ============================================================
fun handleRent(senderAddr: address, msgValue: int) {
    assert(state == STATE_LISTED) throw ERR_LISTING_NOT_ACTIVE;
    assert(nftReceived != 0) throw ERR_NFT_NOT_RECEIVED;
    assert(msgValue >= rentalPrice + GAS_CHANGE_RECORD) throw ERR_INSUFFICIENT_PAYMENT;

    renterAddress = senderAddr;
    var newEndTime: int = blockchain.now() + rentalDuration;
    assert(newEndTime <= 0xFFFFFFFF) throw ERR_OVERFLOW;  // uint32 overflow check
    rentalEndTime = newEndTime;
    state = STATE_RENTED;
    saveStorage();

    // Forward rental price to owner
    sendTons(ownerAddress, rentalPrice);

    // Reserve storage fees, send excess back to renter
    reserveToncoinsOnBalance(MIN_TONS_FOR_STORAGE, RESERVE_MODE_AT_MOST);
    sendExcess(senderAddr);
}

// ============================================================
// OP_CHANGE_RECORD — Renter changes DNS record (proxy)
// ============================================================
fun handleChangeRecord(senderAddr: address, msgBody: slice) {
    assert(state == STATE_RENTED) throw ERR_NOT_RENTED;
    assert(senderAddr == renterAddress!!) throw ERR_NOT_RENTER;
    assert(blockchain.now() < rentalEndTime) throw ERR_RENTAL_EXPIRED;

    var queryId: int = msgBody.loadUint(64);
    var recordKey: int = msgBody.loadUint(256);

    // Build proxy message: forward record_value (remaining slice) as-is
    var msg: builder = beginCell()
        .storeUint(0x18, 6)
        .storeAddress(nftAddress)
        .storeCoins(GAS_CHANGE_RECORD)
        .storeUint(0, 1 + 4 + 4 + 64 + 32 + 1 + 1)
        .storeUint(OP_CHANGE_DNS_RECORD, 32)
        .storeUint(queryId, 64)
        .storeUint(recordKey, 256);

    // Append remaining body (Maybe ^Cell for record_value)
    msg = msg.storeSlice(msgBody);

    sendRawMessage(msg.endCell(), 0);

    // Reserve storage + enough for future NFT transfer (claim_back)
    reserveToncoinsOnBalance(MIN_TONS_FOR_STORAGE, RESERVE_MODE_AT_MOST);
    sendExcess(senderAddr);
}

// ============================================================
// OP_CLAIM_BACK — Return NFT after rental expiration
// ============================================================
fun handleClaimBack(myBalance: int, msgValue: int, msgBody: slice) {
    assert(state == STATE_RENTED) throw ERR_NOT_RENTED;
    assert(blockchain.now() >= rentalEndTime) throw ERR_RENTAL_NOT_EXPIRED;
    assert(myBalance + msgValue >= GAS_TRANSFER_NFT + MIN_TONS_FOR_STORAGE) throw ERR_INSUFFICIENT_BALANCE;

    var queryId: int = msgBody.loadUint(64);

    state = STATE_CLOSED;
    renterAddress = null;
    rentalEndTime = 0;
    saveStorage();

    // Transfer NFT back to owner
    sendNftTransfer(queryId, ownerAddress, ownerAddress);
}

// ============================================================
// OP_DELIST — Owner removes listing (no active renter)
// ============================================================
fun handleDelist(senderAddr: address, myBalance: int, msgValue: int, msgBody: slice) {
    assert(senderAddr == ownerAddress) throw ERR_NOT_OWNER;
    assert(state == STATE_LISTED) throw ERR_ACTIVE_RENTAL;
    assert(myBalance + msgValue >= GAS_TRANSFER_NFT + MIN_TONS_FOR_STORAGE) throw ERR_INSUFFICIENT_BALANCE;

    var queryId: int = msgBody.loadUint(64);

    state = STATE_CLOSED;
    saveStorage();

    // Transfer NFT back to owner
    sendNftTransfer(queryId, ownerAddress, ownerAddress);
}

// ============================================================
// OP_RENEW — Renter extends rental
// ============================================================
fun handleRenew(senderAddr: address, msgValue: int) {
    assert(state == STATE_RENTED) throw ERR_NOT_RENTED;
    assert(senderAddr == renterAddress!!) throw ERR_NOT_RENTER;
    assert(blockchain.now() < rentalEndTime) throw ERR_RENTAL_EXPIRED;
    assert(msgValue >= rentalPrice) throw ERR_INSUFFICIENT_PAYMENT;
    assert(renewalAllowed != 0) throw ERR_RENEWAL_DISABLED;

    var newEndTime: int = rentalEndTime + rentalDuration;
    assert(newEndTime <= blockchain.now() + 31536000) throw ERR_OVERFLOW;  // max 1 year from now
    rentalEndTime = newEndTime;
    saveStorage();

    // Forward rental price to owner
    sendTons(ownerAddress, rentalPrice);

    // Send excess back to renter
    reserveToncoinsOnBalance(MIN_TONS_FOR_STORAGE, RESERVE_MODE_AT_MOST);
    sendExcess(senderAddr);
}

// ============================================================
// OP_STOP_RENEWAL — Owner disables renewals (préavis)
// ============================================================
fun handleStopRenewal(senderAddr: address) {
    assert(senderAddr == ownerAddress) throw ERR_NOT_OWNER;
    assert(state == STATE_RENTED) throw ERR_NOT_RENTED;

    renewalAllowed = 0;  // false
    saveStorage();

    reserveToncoinsOnBalance(MIN_TONS_FOR_STORAGE, RESERVE_MODE_AT_MOST);
    sendExcess(senderAddr);
}

// ============================================================
// OP_WITHDRAW_EXCESS — Owner withdraws stuck funds
// ============================================================
fun handleWithdrawExcess(senderAddr: address) {
    assert(senderAddr == ownerAddress) throw ERR_NOT_OWNER;
    // During active rental, reserve enough for the NFT transfer on claim_back
    var reserveAmount: int = MIN_TONS_FOR_STORAGE;
    if (state == STATE_RENTED) {
        reserveAmount = MIN_TONS_FOR_STORAGE + GAS_TRANSFER_NFT;
    }
    reserveToncoinsOnBalance(reserveAmount, RESERVE_MODE_AT_MOST);
    sendExcess(ownerAddress);
}

// ============================================================
// OP_RELIST — Owner re-lists after CLOSED state
// ============================================================
fun handleRelist(senderAddr: address, msgBody: slice) {
    assert(senderAddr == ownerAddress) throw ERR_NOT_OWNER;
    assert(state == STATE_CLOSED) throw ERR_LISTING_NOT_ACTIVE;

    var queryId: int = msgBody.loadUint(64);

    state = STATE_AWAITING_NFT;
    nftReceived = 0;
    renterAddress = null;
    rentalEndTime = 0;
    renewalAllowed = -1;  // reset to true
    saveStorage();

    reserveToncoinsOnBalance(MIN_TONS_FOR_STORAGE, RESERVE_MODE_AT_MOST);
    sendExcess(senderAddr);
}

// ============================================================
// Message router
// ============================================================
fun onInternalMessage(myBalance: int, msgValue: int, msgFull: cell, msgBody: slice) {
    var cs: slice = msgFull.beginParse();
    var flags: int = cs.loadUint(4);

    // Handle bounced messages
    if (flags & 1) {
        // SECURITY: Verify bounce sender before processing
        var bounceSender: address = cs.loadAddress();

        if (msgBody.remainingBitsCount() >= 32) {
            var bouncedOp: int = msgBody.loadUint(32);
            if (bouncedOp == 0xFFFFFFFF) {
                if (msgBody.remainingBitsCount() >= 32) {
                    var origOp: int = msgBody.loadUint(32);
                    if (origOp == OP_TRANSFER) {
                        // NFT transfer bounced — verify it came from the actual NFT
                        loadStorage();
                        if (state == STATE_CLOSED && bounceSender == nftAddress) {
                            state = STATE_LISTED;
                            nftReceived = -1;
                            saveStorage();
                        }
                    }
                }
            }
        }
        return;
    }

    var senderAddr: address = cs.loadAddress();

    // Accept empty messages (simple TON transfers for storage funding)
    if (msgBody.isEmpty()) {
        return;
    }

    var op: int = msgBody.loadUint(32);

    // Accept comment messages (op == 0)
    if (op == 0) {
        return;
    }

    loadStorage();

    if (op == OP_OWNERSHIP_ASSIGNED) {
        return handleOwnershipAssigned(senderAddr, msgBody);
    }
    if (op == OP_RENT) {
        return handleRent(senderAddr, msgValue);
    }
    if (op == OP_CHANGE_RECORD) {
        return handleChangeRecord(senderAddr, msgBody);
    }
    if (op == OP_CLAIM_BACK) {
        return handleClaimBack(myBalance, msgValue, msgBody);
    }
    if (op == OP_DELIST) {
        return handleDelist(senderAddr, myBalance, msgValue, msgBody);
    }
    if (op == OP_RENEW) {
        return handleRenew(senderAddr, msgValue);
    }
    if (op == OP_STOP_RENEWAL) {
        return handleStopRenewal(senderAddr);
    }
    if (op == OP_WITHDRAW_EXCESS) {
        return handleWithdrawExcess(senderAddr);
    }
    if (op == OP_RELIST) {
        return handleRelist(senderAddr, msgBody);
    }

    // SECURITY: Block any attempt to directly call NFT ops through this contract
    // The renter must NEVER be able to transfer the NFT or directly change DNS records
    // bypassing our proxy. These opcodes must be rejected explicitly.
    assert(op != OP_TRANSFER) throw ERR_FORBIDDEN_OP;
    assert(op != OP_CHANGE_DNS_RECORD) throw ERR_FORBIDDEN_OP;

    // Unknown opcodes: silently accept (gas donations, etc.)
}

// ============================================================
// GET methods
// ============================================================
get fun get_listing_data(): (int, address, address, address?, int, int, int, int, int) {
    loadStorage();
    return (
        state,
        nftAddress,
        ownerAddress,
        renterAddress,
        rentalPrice,
        rentalDuration,
        rentalEndTime,
        nftReceived,
        renewalAllowed
    );
}

get fun get_rental_status(): (int, int, int) {
    loadStorage();
    var remaining: int = 0;
    if (state == STATE_RENTED) {
        remaining = rentalEndTime - blockchain.now();
        if (remaining < 0) {
            remaining = 0;
        }
    }
    return (state, rentalEndTime, remaining);
}
