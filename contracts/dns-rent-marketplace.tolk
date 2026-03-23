tolk 1.2

import "@stdlib/gas-payments"
import "errors.tolk"
import "messages.tolk"

// ============================================================
// Global storage variables
// ============================================================
global ownerAddress: address;
global listingCode: cell;
global nextListingIndex: int;

// ============================================================
// Storage load / save
// ============================================================
fun loadStorage() {
    var ds: slice = contract.getData().beginParse();
    ownerAddress = ds.loadAddress();
    listingCode = ds.loadRef();
    nextListingIndex = ds.loadUint(64);
}

fun saveStorage() {
    contract.setData(
        beginCell()
            .storeAddress(ownerAddress)
            .storeRef(listingCode)
            .storeUint(nextListingIndex, 64)
            .endCell()
    );
}

// ============================================================
// Listing deployment helpers
// ============================================================

// Build stateInit for a DnsRentListing contract
fun calculateListingStateInit(nftAddr: address, ownerAddr: address, price: int, duration: int): cell {
    var rentalRef: cell = beginCell()
        .storeUint(0, 2)                 // renter_address = addr_none
        .storeCoins(price)               // rental_price
        .storeUint(duration, 32)         // rental_duration
        .storeUint(0, 32)                // rental_end_time = 0
        .storeInt(-1, 1)                 // renewal_allowed = true
        .endCell();
    var data: cell = beginCell()
        .storeAddress(contract.getAddress())       // marketplace_addr
        .storeAddress(nftAddr)           // nft_address
        .storeAddress(ownerAddr)         // owner_address
        .storeUint(STATE_AWAITING_NFT, 8) // state
        .storeInt(0, 1)                  // nft_received = false
        .storeRef(rentalRef)             // rental fields ref
        .endCell();

    return beginCell()
        .storeUint(0, 2)       // no split_depth, no special
        .storeUint(1, 1)       // has code
        .storeRef(listingCode)
        .storeUint(1, 1)       // has data
        .storeRef(data)
        .storeUint(0, 1)       // no library
        .endCell();
}

// Compute deterministic address from stateInit
fun calculateListingAddress(stateInit: cell): slice {
    return beginCell()
        .storeUint(4, 3)              // addr_std$10 + anycast = 0
        .storeUint(0, 8)              // workchain = 0
        .storeUint(stateInit.hash(), 256)
        .endCell()
        .beginParse();
}

// ============================================================
// Message router
// ============================================================
fun onInternalMessage(myBalance: int, msgValue: int, msgFull: cell, msgBody: slice) {
    // Parse sender and flags first
    var cs: slice = msgFull.beginParse();
    var flags: int = cs.loadUint(4);

    // Ignore bounced messages
    if (flags & 1) {
        return;
    }

    var senderAddr: address = cs.loadAddress();

    // Accept empty messages (simple TON transfers for storage fees)
    if (msgBody.isEmpty()) {
        return;
    }

    var op: int = msgBody.loadUint(32);

    // Accept messages with op=0 (text comments)
    if (op == 0) {
        return;
    }

    if (op == OP_OWNERSHIP_ASSIGNED) {
        // NFT transferred to marketplace = listing request
        var queryId: int = msgBody.loadUint(64);
        var prevOwner: address = msgBody.loadAddress();

        // Parse listing params from forward_payload
        // forward_payload is Either Cell ^Cell - check the bit
        var isRef: int = msgBody.loadUint(1);
        var params: slice;
        if (isRef) {
            params = msgBody.loadRef().beginParse();
        } else {
            params = msgBody;
        }

        var rentalPrice: int = params.loadCoins();
        var rentalDuration: int = params.loadUint(32);

        // Ensure caller pays for listing deployment
        var minRequired: int = MIN_TONS_FOR_STORAGE + GAS_TRANSFER_NFT + GAS_NOTIFICATION + GAS_NOTIFICATION;
        assert(msgValue >= minRequired) throw ERR_INVALID_PARAMS;

        // Validate parameters
        assert(rentalPrice >= MIN_TONS_FOR_STORAGE) throw ERR_ZERO_PRICE;
        assert(rentalDuration > 0) throw ERR_ZERO_DURATION;
        assert(rentalDuration <= 31536000) throw ERR_INVALID_PARAMS;  // max 1 year

        loadStorage();

        // nftAddress = sender of ownership_assigned (the NFT contract)
        var nftAddr: address = senderAddr;

        // Build stateInit and compute listing address
        var stateInit: cell = calculateListingStateInit(nftAddr, prevOwner, rentalPrice, rentalDuration);
        var listingAddrSlice: slice = calculateListingAddress(stateInit);

        // Deploy listing contract with stateInit
        var deployBody: cell = beginCell()
            .storeUint(OP_LISTING_CREATED, 32)
            .storeUint(queryId, 64)
            .endCell();

        var deployMsg: cell = beginCell()
            .storeUint(0x18, 6)
            .storeSlice(listingAddrSlice)
            .storeCoins(MIN_TONS_FOR_STORAGE)
            .storeUint(0, 1 + 4 + 4 + 64 + 32)
            .storeUint(1, 1)   // has state_init
            .storeUint(1, 1)   // state_init in ref
            .storeRef(stateInit)
            .storeUint(1, 1)   // body in ref
            .storeRef(deployBody)
            .endCell();
        sendRawMessage(deployMsg, SEND_MODE_REGULAR);

        // Transfer NFT from Marketplace to Listing
        var transferBody: cell = beginCell()
            .storeUint(OP_TRANSFER, 32)
            .storeUint(queryId, 64)
            .storeSlice(listingAddrSlice)     // new_owner = listing address (as slice)
            .storeAddress(prevOwner)          // response_destination = original owner
            .storeUint(0, 1)                  // no custom_payload
            .storeCoins(GAS_NOTIFICATION)     // forward_amount
            .storeUint(0, 1)                  // no forward_payload
            .endCell();

        var transferMsg: cell = beginCell()
            .storeUint(0x18, 6)
            .storeAddress(nftAddr)            // send to NFT contract
            .storeCoins(GAS_TRANSFER_NFT)     // value for the transfer
            .storeUint(0, 1 + 4 + 4 + 64 + 32)
            .storeUint(0, 1)   // no state_init
            .storeUint(1, 1)   // body in ref
            .storeRef(transferBody)
            .endCell();
        sendRawMessage(transferMsg, SEND_MODE_REGULAR);

        // Send notification to original owner with listing address
        var notification: cell = beginCell()
            .storeUint(0x10, 6)
            .storeAddress(prevOwner)
            .storeCoins(GAS_NOTIFICATION)
            .storeUint(0, 1 + 4 + 4 + 64 + 32 + 1 + 1)
            .storeUint(OP_LISTING_CREATED, 32)
            .storeUint(queryId, 64)
            .storeSlice(listingAddrSlice)
            .endCell();
        sendRawMessage(notification, SEND_MODE_IGNORE_ERRORS);

        // Update marketplace state
        nextListingIndex += 1;
        saveStorage();
        return;
    }

    if (op == OP_EMERGENCY_RETURN) {
        loadStorage();
        assert(senderAddr == ownerAddress) throw ERR_NOT_MARKETPLACE_OWNER;
        var queryId: int = msgBody.loadUint(64);
        var nftAddr: address = msgBody.loadAddress();
        var returnTo: address = msgBody.loadAddress();

        var body: cell = beginCell()
            .storeUint(OP_TRANSFER, 32)
            .storeUint(queryId, 64)
            .storeAddress(returnTo)
            .storeAddress(returnTo)
            .storeUint(0, 1)
            .storeCoins(0)
            .storeUint(0, 1)
            .endCell();

        sendRawMessage(
            beginCell()
                .storeUint(0x18, 6)
                .storeAddress(nftAddr)
                .storeCoins(GAS_TRANSFER_NFT)
                .storeUint(0, 1 + 4 + 4 + 64 + 32)
                .storeUint(0, 1)   // no state_init
                .storeUint(1, 1)   // body in ref
                .storeRef(body)
                .endCell(),
            0
        );
        return;
    }

    // Unknown opcode
    throw 0xffff;
}

// ============================================================
// GET methods
// ============================================================
get fun get_marketplace_data(): (address, int) {
    loadStorage();
    return (ownerAddress, nextListingIndex);
}

get fun get_listing_address(nftAddr: address, ownerAddr: address, price: int, duration: int): slice {
    loadStorage();
    var stateInit: cell = calculateListingStateInit(nftAddr, ownerAddr, price, duration);
    return calculateListingAddress(stateInit);
}
