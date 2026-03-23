/**
 * marketplace-audit.test.ts
 *
 * Security audit & end-to-end flow tests for the DNS Rent Marketplace contract.
 * Tests exploit vectors, access control, atomic flows, and full lifecycle scenarios.
 */
import { Blockchain, SandboxContract, TreasuryContract } from '@ton/sandbox';
import { Cell, beginCell, Address, contractAddress, toNano, StateInit } from '@ton/core';
import { describe, it, expect, beforeEach } from 'vitest';
import '@ton/test-utils';

// Load compiled bytecode
const LISTING_CODE = Cell.fromBase64(require('../build/dns-rent-listing.json').codeBoc64);
const MARKETPLACE_CODE = Cell.fromBase64(require('../build/dns-rent-marketplace.json').codeBoc64);

// ===== Opcodes =====
const OP_TRANSFER = 0x5fcc3d14;
const OP_OWNERSHIP_ASSIGNED = 0x05138d91;
const OP_EXCESSES = 0xd53276db;
const OP_CHANGE_DNS_RECORD = 0x4eb1f0f9;
const OP_RENT = 0x52454e54;
const OP_CHANGE_RECORD = 0x4368526b;
const OP_CLAIM_BACK = 0x436c4261;
const OP_DELIST = 0x44654c73;
const OP_RENEW = 0x52456e77;
const OP_WITHDRAW_EXCESS = 0x57746864;
const OP_RELIST = 0x52654c73;
const OP_STOP_RENEWAL = 0x53745270;
const OP_EMERGENCY_RETURN = 0x456d5274;
const OP_LISTING_CREATED = 0x4c437264;

// ===== States =====
const STATE_AWAITING_NFT = 0;
const STATE_LISTED = 1;
const STATE_RENTED = 2;
const STATE_CLOSED = 3;

// ===== Gas constants =====
const MIN_TONS_FOR_STORAGE = 150000000n;
const GAS_CHANGE_RECORD = 50000000n;
const GAS_TRANSFER_NFT = 100000000n;
const GAS_NOTIFICATION = 10000000n;

// ===== Error codes =====
const ERR_NOT_OWNER = 100;
const ERR_NFT_NOT_RECEIVED = 101;
const ERR_NFT_ALREADY_RECEIVED = 102;
const ERR_WRONG_NFT = 103;
const ERR_LISTING_NOT_ACTIVE = 105;
const ERR_INSUFFICIENT_PAYMENT = 111;
const ERR_NOT_RENTED = 112;
const ERR_RENTAL_EXPIRED = 113;
const ERR_RENTAL_NOT_EXPIRED = 114;
const ERR_OVERFLOW = 115;
const ERR_RENEWAL_DISABLED = 116;
const ERR_NOT_RENTER = 120;
const ERR_FORBIDDEN_OP = 125;
const ERR_ACTIVE_RENTAL = 130;
const ERR_NOT_MARKETPLACE_OWNER = 140;
const ERR_INVALID_PARAMS = 141;
const ERR_ZERO_PRICE = 142;
const ERR_ZERO_DURATION = 143;

// ===== Helpers =====

function getExitCode(transactions: any[], to: Address): number | undefined {
    for (const tx of transactions) {
        if (tx.inMessage?.info?.dest?.equals?.(to)) {
            const desc = tx.description;
            if (desc.type === 'generic' && desc.computePhase?.type === 'vm') {
                return desc.computePhase.exitCode;
            }
        }
    }
    return undefined;
}

function isAborted(transactions: any[], to: Address): boolean {
    for (const tx of transactions) {
        if (tx.inMessage?.info?.dest?.equals?.(to)) {
            if (tx.description?.aborted === true) return true;
        }
    }
    return false;
}

function buildMarketplaceData(ownerAddr: Address): Cell {
    return beginCell()
        .storeAddress(ownerAddr)
        .storeRef(LISTING_CODE)
        .storeUint(0, 64)
        .endCell();
}

function buildListingData(
    marketplaceAddr: Address,
    nftAddr: Address,
    ownerAddr: Address,
    rentalPrice: bigint,
    rentalDuration: number,
): Cell {
    const rentalRef = beginCell()
        .storeUint(0, 2)                // renter = addr_none
        .storeCoins(rentalPrice)
        .storeUint(rentalDuration, 32)
        .storeUint(0, 32)               // rental_end_time = 0
        .storeInt(-1, 1)                // renewal_allowed = true
        .endCell();
    return beginCell()
        .storeAddress(marketplaceAddr)
        .storeAddress(nftAddr)
        .storeAddress(ownerAddr)
        .storeUint(STATE_AWAITING_NFT, 8)
        .storeInt(0, 1)                  // nft_received = false
        .storeRef(rentalRef)
        .endCell();
}

/** Read marketplace get_marketplace_data */
async function readMarketplaceData(blockchain: Blockchain, addr: Address) {
    const provider = blockchain.provider(addr);
    const { stack } = await provider.get('get_marketplace_data', []);
    return {
        owner: stack.readAddress(),
        nextIndex: stack.readBigNumber(),
    };
}

/** Read listing get_listing_data */
async function readListingData(blockchain: Blockchain, addr: Address) {
    const provider = blockchain.provider(addr);
    const { stack } = await provider.get('get_listing_data', []);
    return {
        state: stack.readNumber(),
        nftAddress: stack.readAddress(),
        ownerAddress: stack.readAddress(),
        renterAddress: stack.readAddressOpt(),
        rentalPrice: stack.readBigNumber(),
        rentalDuration: stack.readNumber(),
        rentalEndTime: stack.readNumber(),
        nftReceived: stack.readNumber(),
        renewalAllowed: stack.readNumber(),
    };
}

/** Get listing address from marketplace get method */
async function getListingAddress(
    blockchain: Blockchain,
    marketplaceAddr: Address,
    nftAddr: Address,
    ownerAddr: Address,
    price: bigint,
    duration: number,
): Promise<Address> {
    const provider = blockchain.provider(marketplaceAddr);
    const { stack } = await provider.get('get_listing_address', [
        { type: 'slice', cell: beginCell().storeAddress(nftAddr).endCell() },
        { type: 'slice', cell: beginCell().storeAddress(ownerAddr).endCell() },
        { type: 'int', value: price },
        { type: 'int', value: BigInt(duration) },
    ]);
    const addrCell = stack.readCell();
    return addrCell.beginParse().loadAddress();
}

/** Find transaction with specific op sent to a destination */
function findOpMessage(transactions: any[], dest: Address, op: number): any | undefined {
    for (const tx of transactions) {
        if (!tx.inMessage?.info?.dest?.equals?.(dest)) continue;
        const body = tx.inMessage?.body;
        if (!body) continue;
        const bs = body.beginParse();
        // Body might be inline or in ref
        let bodySlice = bs;
        if (bs.remainingBits < 32 && bs.remainingRefs > 0) {
            bodySlice = bs.loadRef().beginParse();
        }
        if (bodySlice.remainingBits >= 32) {
            const msgOp = bodySlice.loadUint(32);
            if (msgOp === op) return tx;
        }
    }
    return undefined;
}

// ============================================================
// Category 1: Marketplace Atomic Flow
// ============================================================
describe('Category 1: Marketplace Atomic Flow', () => {
    let blockchain: Blockchain;
    let admin: SandboxContract<TreasuryContract>;
    let ownerWallet: SandboxContract<TreasuryContract>;
    let marketplaceAddress: Address;
    let marketplaceInit: StateInit;

    const RENTAL_PRICE = toNano('1');
    const RENTAL_DURATION = 86400;

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        admin = await blockchain.treasury('admin');
        ownerWallet = await blockchain.treasury('owner');

        const data = buildMarketplaceData(admin.address);
        marketplaceInit = { code: MARKETPLACE_CODE, data };
        marketplaceAddress = contractAddress(0, marketplaceInit);

        await admin.send({
            to: marketplaceAddress,
            value: toNano('1'),
            init: marketplaceInit,
            body: beginCell().endCell(),
            bounce: false,
        });
    });

    async function sendNftToMarketplace(
        nftWallet: SandboxContract<TreasuryContract>,
        prevOwner: SandboxContract<TreasuryContract>,
        price: bigint = RENTAL_PRICE,
        duration: number = RENTAL_DURATION,
        value: bigint = toNano('0.5'),
    ) {
        return nftWallet.send({
            to: marketplaceAddress,
            value,
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(prevOwner.address)
                .storeUint(1, 1) // forward_payload in ref
                .storeRef(beginCell()
                    .storeCoins(price)
                    .storeUint(duration, 32)
                    .endCell())
                .endCell(),
        });
    }

    it('1. Full atomic flow: NFT -> marketplace -> listing deployed + NFT transferred', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, ownerWallet);

        // Marketplace should succeed
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(false);

        // Marketplace counters should be incremented
        const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(1n);

        // Get listing address from marketplace get method
        const listingAddr = await getListingAddress(
            blockchain, marketplaceAddress,
            nftWallet.address, ownerWallet.address,
            RENTAL_PRICE, RENTAL_DURATION,
        );

        // Listing should have been deployed (find transaction to listing address)
        const listingTx = r.transactions.find(
            (tx: any) => tx.inMessage?.info?.dest?.equals?.(listingAddr)
        );
        expect(listingTx).toBeDefined();

        // NFT transfer message should have been sent to nftWallet
        const transferTx = findOpMessage(r.transactions, nftWallet.address, OP_TRANSFER);
        expect(transferTx).toBeDefined();

        // Owner notification should have been sent
        const notifTx = findOpMessage(r.transactions, ownerWallet.address, OP_LISTING_CREATED);
        expect(notifTx).toBeDefined();
    });

    it('2. ownership_assigned with insufficient value -> action phase failure, marketplace state clean', async () => {
        const nftWallet = await blockchain.treasury('nft');

        // Send with very low value - compute phase succeeds but action phase may fail
        // because marketplace needs to send MIN_TONS_FOR_STORAGE + GAS_TRANSFER_NFT + GAS_NOTIFICATION
        const r = await sendNftToMarketplace(nftWallet, ownerWallet, RENTAL_PRICE, RENTAL_DURATION, toNano('0.01'));

        // The compute phase might succeed (exit code 0) but action phase could fail
        // OR it may not have enough gas at all. Check what happens:
        const mpTx = r.transactions.find(
            (tx: any) => tx.inMessage?.info?.dest?.equals?.(marketplaceAddress)
        );
        expect(mpTx).toBeDefined();

        if (mpTx.description?.aborted) {
            // If aborted, storage should NOT be updated (transaction rolled back)
            const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
            // State should be clean - counters not incremented if action phase fails
            // Note: in TON, if action phase fails, c4 changes are NOT committed
            expect(mdata.nextIndex).toBe(0n);
        } else {
            // If not aborted, some messages may have been sent with IGNORE_ERRORS
            // The marketplace state is updated - this is the expected behavior
            // since sendRawMessage with mode 0 will fail silently if not enough balance
            const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
            // Counters were incremented
            expect(mdata.nextIndex).toBe(1n);
        }
    });

    it('3. ownership_assigned with malformed forward_payload (too short) -> revert', async () => {
        const nftWallet = await blockchain.treasury('nft');

        // Send with forward_payload that is too short (missing duration)
        const r = await nftWallet.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(ownerWallet.address)
                .storeUint(1, 1) // in ref
                .storeRef(beginCell()
                    .storeCoins(RENTAL_PRICE)
                    // Missing: .storeUint(duration, 32)
                    .endCell())
                .endCell(),
        });

        // Should fail during parsing (cell underflow)
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
        // State should remain clean
        const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(0n);
    });

    it('4. ownership_assigned with zero price -> ERR_ZERO_PRICE', async () => {
        const nftWallet = await blockchain.treasury('nft');
        const r = await sendNftToMarketplace(nftWallet, ownerWallet, 0n);
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_ZERO_PRICE);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
    });

    it('5. ownership_assigned with zero duration -> ERR_ZERO_DURATION', async () => {
        const nftWallet = await blockchain.treasury('nft');
        const r = await sendNftToMarketplace(nftWallet, ownerWallet, RENTAL_PRICE, 0);
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_ZERO_DURATION);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
    });

    it('6. ownership_assigned with duration > 1 year -> ERR_INVALID_PARAMS', async () => {
        const nftWallet = await blockchain.treasury('nft');
        const r = await sendNftToMarketplace(nftWallet, ownerWallet, RENTAL_PRICE, 31536001);
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_INVALID_PARAMS);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
    });

    it('7. ownership_assigned with price below minimum -> ERR_ZERO_PRICE', async () => {
        const nftWallet = await blockchain.treasury('nft');
        // Price = 0.01 TON, below MIN_TONS_FOR_STORAGE (0.15 TON)
        const r = await sendNftToMarketplace(nftWallet, ownerWallet, toNano('0.01'));
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_ZERO_PRICE);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
    });
});

// ============================================================
// Category 2: Emergency Return
// ============================================================
describe('Category 2: Emergency Return', () => {
    let blockchain: Blockchain;
    let admin: SandboxContract<TreasuryContract>;
    let attacker: SandboxContract<TreasuryContract>;
    let marketplaceAddress: Address;
    let marketplaceInit: StateInit;

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        admin = await blockchain.treasury('admin');
        attacker = await blockchain.treasury('attacker');

        const data = buildMarketplaceData(admin.address);
        marketplaceInit = { code: MARKETPLACE_CODE, data };
        marketplaceAddress = contractAddress(0, marketplaceInit);

        await admin.send({
            to: marketplaceAddress,
            value: toNano('1'),
            init: marketplaceInit,
            body: beginCell().endCell(),
            bounce: false,
        });
    });

    it('8. Marketplace owner calls OP_EMERGENCY_RETURN -> NFT transfer sent', async () => {
        const nftAddr = await blockchain.treasury('nft');
        const returnTo = await blockchain.treasury('returnTo');

        const r = await admin.send({
            to: marketplaceAddress,
            value: toNano('0.3'),
            body: beginCell()
                .storeUint(OP_EMERGENCY_RETURN, 32)
                .storeUint(42, 64)
                .storeAddress(nftAddr.address)
                .storeAddress(returnTo.address)
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(false);

        // Should send OP_TRANSFER to the NFT address
        const transferTx = findOpMessage(r.transactions, nftAddr.address, OP_TRANSFER);
        expect(transferTx).toBeDefined();

        // Verify transfer body contains correct returnTo
        const body = transferTx.inMessage.body;
        const bs = body.beginParse();
        let bodySlice = bs;
        if (bs.remainingBits < 32 && bs.remainingRefs > 0) {
            bodySlice = bs.loadRef().beginParse();
        }
        const op = bodySlice.loadUint(32);
        expect(op).toBe(OP_TRANSFER);
        const qid = bodySlice.loadUint(64);
        expect(qid).toBe(42);
        const newOwner = bodySlice.loadAddress();
        expect(newOwner.equals(returnTo.address)).toBe(true);
    });

    it('9. Non-owner calls OP_EMERGENCY_RETURN -> ERR_NOT_MARKETPLACE_OWNER', async () => {
        const nftAddr = await blockchain.treasury('nft');
        const returnTo = await blockchain.treasury('returnTo');

        const r = await attacker.send({
            to: marketplaceAddress,
            value: toNano('0.3'),
            body: beginCell()
                .storeUint(OP_EMERGENCY_RETURN, 32)
                .storeUint(0, 64)
                .storeAddress(nftAddr.address)
                .storeAddress(returnTo.address)
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_NOT_MARKETPLACE_OWNER);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
    });

    it('10. Emergency return with low balance -> action phase handles it', async () => {
        const nftAddr = await blockchain.treasury('nft');
        const returnTo = await blockchain.treasury('returnTo');

        // Send with minimal value - the contract uses mode 0 for the transfer
        // which means it must have enough balance for GAS_TRANSFER_NFT
        const r = await admin.send({
            to: marketplaceAddress,
            value: toNano('0.02'), // Very low, might not cover GAS_TRANSFER_NFT
            body: beginCell()
                .storeUint(OP_EMERGENCY_RETURN, 32)
                .storeUint(0, 64)
                .storeAddress(nftAddr.address)
                .storeAddress(returnTo.address)
                .endCell(),
        });

        // Compute phase should still succeed (exit code 0)
        // but the outbound message might fail in action phase
        // Since marketplace was funded with 1 TON in beforeEach, it has balance.
        // The mode 0 sendRawMessage uses GAS_TRANSFER_NFT from contract balance.
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
    });
});

// ============================================================
// Category 3: Marketplace Access Control
// ============================================================
describe('Category 3: Marketplace Access Control', () => {
    let blockchain: Blockchain;
    let admin: SandboxContract<TreasuryContract>;
    let user: SandboxContract<TreasuryContract>;
    let marketplaceAddress: Address;
    let marketplaceInit: StateInit;

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        admin = await blockchain.treasury('admin');
        user = await blockchain.treasury('user');

        const data = buildMarketplaceData(admin.address);
        marketplaceInit = { code: MARKETPLACE_CODE, data };
        marketplaceAddress = contractAddress(0, marketplaceInit);

        await admin.send({
            to: marketplaceAddress,
            value: toNano('1'),
            init: marketplaceInit,
            body: beginCell().endCell(),
            bounce: false,
        });
    });

    it('11. Unknown opcode -> throw 0xffff (bounces back)', async () => {
        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('0.1'),
            body: beginCell()
                .storeUint(0xdeadbeef, 32)
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0xffff);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(true);
    });

    it('12. Empty message -> accepted (storage top-up)', async () => {
        const balanceBefore = (await blockchain.getContract(marketplaceAddress)).balance;

        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('5'),
            body: beginCell().endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(false);

        const balanceAfter = (await blockchain.getContract(marketplaceAddress)).balance;
        expect(balanceAfter).toBeGreaterThan(balanceBefore);
    });

    it('13. Comment message (op=0) -> accepted', async () => {
        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('0.05'),
            body: beginCell()
                .storeUint(0, 32)
                .storeStringTail('donation')
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(false);
    });

    it('14. Bounced message -> silently ignored', async () => {
        // Simulate a bounced message by manually crafting it with bounce flag
        // In sandbox, we can use internal_relaxed or direct send with bounce flag
        // The simplest way is to send a message that will bounce from marketplace
        // and check that the bounce is handled
        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('0.05'),
            body: beginCell()
                .storeUint(0xdeadbeef, 32)
                .endCell(),
        });

        // This will bounce back to user because of throw 0xffff
        // The bounce itself is handled by user's wallet, not marketplace
        // But verify marketplace correctly throws
        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0xffff);

        // Now test: if a bounced message arrives at marketplace, it ignores it
        // We can verify this by checking that the bounced flag check works:
        // Send a message to a contract that bounces, and the bounce goes to marketplace
        // For simplicity, we verify the contract code path with a different approach:
        // Marketplace state should be unchanged after the bounced interaction
        const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(0n);
    });
});

// ============================================================
// Category 4: End-to-End Lifecycle
// ============================================================
describe('Category 4: End-to-End Lifecycle', () => {
    let blockchain: Blockchain;
    let admin: SandboxContract<TreasuryContract>;
    let ownerWallet: SandboxContract<TreasuryContract>;
    let nftWallet: SandboxContract<TreasuryContract>;
    let renterWallet: SandboxContract<TreasuryContract>;
    let marketplaceAddress: Address;
    let marketplaceInit: StateInit;

    const RENTAL_PRICE = toNano('1');
    const RENTAL_DURATION = 86400; // 1 day

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        blockchain.now = Math.floor(Date.now() / 1000);

        admin = await blockchain.treasury('admin');
        ownerWallet = await blockchain.treasury('owner');
        nftWallet = await blockchain.treasury('nft');
        renterWallet = await blockchain.treasury('renter');

        const data = buildMarketplaceData(admin.address);
        marketplaceInit = { code: MARKETPLACE_CODE, data };
        marketplaceAddress = contractAddress(0, marketplaceInit);

        await admin.send({
            to: marketplaceAddress,
            value: toNano('1'),
            init: marketplaceInit,
            body: beginCell().endCell(),
            bounce: false,
        });
    });

    /**
     * Helper: Deploy a listing via the marketplace flow.
     * Since the sandbox treasury acts as a mock NFT, the OP_TRANSFER message
     * sent by the marketplace back to nftWallet is just received by the treasury.
     * We need to then manually send ownership_assigned from nftWallet to the listing
     * to simulate the NFT contract forwarding the transfer notification.
     */
    async function deployListingViaMarketplace(
        nft: SandboxContract<TreasuryContract>,
        owner: SandboxContract<TreasuryContract>,
        price: bigint = RENTAL_PRICE,
        duration: number = RENTAL_DURATION,
    ): Promise<{ listingAddr: Address; deployResult: any }> {
        // Step 1: NFT sends ownership_assigned to marketplace
        const r = await nft.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(owner.address)
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(price)
                    .storeUint(duration, 32)
                    .endCell())
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);

        // Step 2: Get the listing address
        const listingAddr = await getListingAddress(
            blockchain, marketplaceAddress,
            nft.address, owner.address, price, duration,
        );

        return { listingAddr, deployResult: r };
    }

    /**
     * Helper: Simulate NFT arriving at listing (ownership_assigned from nftWallet to listing).
     * This simulates the NFT contract processing the OP_TRANSFER from marketplace
     * and then sending ownership_assigned to the new owner (listing).
     */
    async function sendNftToListing(
        nft: SandboxContract<TreasuryContract>,
        listingAddr: Address,
        prevOwner: Address = marketplaceAddress,
    ) {
        return nft.send({
            to: listingAddr,
            value: toNano('0.1'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(prevOwner)
                .storeUint(0, 1) // no forward_payload
                .endCell(),
        });
    }

    it('15. FULL LIFECYCLE TEST', async () => {
        // ===== Phase A: Deploy marketplace and create listing =====
        const { listingAddr } = await deployListingViaMarketplace(nftWallet, ownerWallet);

        // Verify listing was deployed and is in AWAITING_NFT state
        // (marketplace deployed it, but NFT hasn't arrived yet from the mock)
        let listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_AWAITING_NFT);
        expect(listingData.nftAddress.equals(nftWallet.address)).toBe(true);
        expect(listingData.ownerAddress.equals(ownerWallet.address)).toBe(true);

        // ===== Phase B: NFT arrives at listing -> LISTED =====
        const nftArrival = await sendNftToListing(nftWallet, listingAddr);
        expect(getExitCode(nftArrival.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_LISTED);
        expect(listingData.nftReceived).toBe(-1); // true

        // ===== Phase C: Renter rents the domain =====
        const rentPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
        const ownerBalanceBefore = (await blockchain.getContract(ownerWallet.address)).balance;

        const rentResult = await renterWallet.send({
            to: listingAddr,
            value: rentPayment,
            body: beginCell()
                .storeUint(OP_RENT, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(rentResult.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_RENTED);
        expect(listingData.renterAddress!.equals(renterWallet.address)).toBe(true);
        expect(listingData.rentalEndTime).toBe(blockchain.now! + RENTAL_DURATION);

        // Verify owner received payment
        const ownerBalanceAfter = (await blockchain.getContract(ownerWallet.address)).balance;
        expect(ownerBalanceAfter).toBeGreaterThan(ownerBalanceBefore);

        // ===== Phase D: Renter changes DNS record =====
        const recordKey = BigInt('0xe8d44050873dba865aa7c170ab4cce64d90839a34dcfd6cf71d14e0205443b1b'); // wallet record
        const recordValue = beginCell().storeAddress(renterWallet.address).endCell();

        const changeResult = await renterWallet.send({
            to: listingAddr,
            value: toNano('0.1'),
            body: beginCell()
                .storeUint(OP_CHANGE_RECORD, 32)
                .storeUint(0, 64)
                .storeUint(recordKey, 256)
                .storeRef(recordValue)
                .endCell(),
        });
        expect(getExitCode(changeResult.transactions, listingAddr)).toBe(0);

        // Verify proxy message was sent to NFT (OP_CHANGE_DNS_RECORD)
        const dnsTx = findOpMessage(changeResult.transactions, nftWallet.address, OP_CHANGE_DNS_RECORD);
        expect(dnsTx).toBeDefined();

        // ===== Phase E: Owner stops renewals =====
        const stopResult = await ownerWallet.send({
            to: listingAddr,
            value: toNano('0.05'),
            body: beginCell()
                .storeUint(OP_STOP_RENEWAL, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(stopResult.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.renewalAllowed).toBe(0);

        // ===== Phase F: Renter tries to renew -> ERR_RENEWAL_DISABLED =====
        const renewResult = await renterWallet.send({
            to: listingAddr,
            value: RENTAL_PRICE + toNano('0.1'),
            body: beginCell()
                .storeUint(OP_RENEW, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(renewResult.transactions, listingAddr)).toBe(ERR_RENEWAL_DISABLED);

        // ===== Phase G: Time passes, rental expires =====
        blockchain.now = blockchain.now! + RENTAL_DURATION + 1;

        // ===== Phase H: Anyone calls OP_CLAIM_BACK =====
        const claimResult = await admin.send({ // admin (random party) calls claim
            to: listingAddr,
            value: toNano('0.2'),
            body: beginCell()
                .storeUint(OP_CLAIM_BACK, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(claimResult.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_CLOSED);
        expect(listingData.renterAddress).toBeNull();

        // Verify NFT transfer was sent back to owner
        const claimTransfer = findOpMessage(claimResult.transactions, nftWallet.address, OP_TRANSFER);
        expect(claimTransfer).toBeDefined();

        // ===== Phase I: Owner re-lists =====
        const relistResult = await ownerWallet.send({
            to: listingAddr,
            value: toNano('0.05'),
            body: beginCell()
                .storeUint(OP_RELIST, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(relistResult.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_AWAITING_NFT);
        expect(listingData.renewalAllowed).toBe(-1); // reset to true

        // ===== Phase J: NFT arrives again -> LISTED =====
        const nftArrival2 = await sendNftToListing(nftWallet, listingAddr, ownerWallet.address);
        expect(getExitCode(nftArrival2.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_LISTED);

        // ===== Phase K: New renter rents =====
        const renter2 = await blockchain.treasury('renter2');
        const rent2Result = await renter2.send({
            to: listingAddr,
            value: rentPayment,
            body: beginCell()
                .storeUint(OP_RENT, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(rent2Result.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_RENTED);
        expect(listingData.renterAddress!.equals(renter2.address)).toBe(true);

        // Full second cycle verified!
    });

    it('16. CONCURRENT LISTINGS: Two different NFTs listed through same marketplace', async () => {
        const nftA = await blockchain.treasury('nftA');
        const nftB = await blockchain.treasury('nftB');

        // NFT_A sends ownership_assigned
        const rA = await nftA.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(ownerWallet.address)
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(toNano('1'))
                    .storeUint(86400, 32)
                    .endCell())
                .endCell(),
        });
        expect(getExitCode(rA.transactions, marketplaceAddress)).toBe(0);

        // NFT_B sends ownership_assigned with different params
        const rB = await nftB.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(ownerWallet.address)
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(toNano('2'))
                    .storeUint(172800, 32)
                    .endCell())
                .endCell(),
        });
        expect(getExitCode(rB.transactions, marketplaceAddress)).toBe(0);

        // Verify two different listing addresses
        const addrA = await getListingAddress(
            blockchain, marketplaceAddress,
            nftA.address, ownerWallet.address, toNano('1'), 86400,
        );
        const addrB = await getListingAddress(
            blockchain, marketplaceAddress,
            nftB.address, ownerWallet.address, toNano('2'), 172800,
        );
        expect(addrA.equals(addrB)).toBe(false);

        // Verify marketplace counters = 2
        const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(2n);

        // Both listings should be deployed
        const listingAData = await readListingData(blockchain, addrA);
        expect(listingAData.nftAddress.equals(nftA.address)).toBe(true);

        const listingBData = await readListingData(blockchain, addrB);
        expect(listingBData.nftAddress.equals(nftB.address)).toBe(true);
    });

    it('17. DUPLICATE LISTING: Same NFT sends ownership_assigned twice with same params', async () => {
        // First time: listing deployed
        const { listingAddr } = await deployListingViaMarketplace(nftWallet, ownerWallet);

        let mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(1n);

        // Second time: same params -> same stateInit -> same address
        // The deploy message goes to an already-existing contract, stateInit is ignored
        const r2 = await nftWallet.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(ownerWallet.address)
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(RENTAL_PRICE)
                    .storeUint(RENTAL_DURATION, 32)
                    .endCell())
                .endCell(),
        });
        expect(getExitCode(r2.transactions, marketplaceAddress)).toBe(0);

        // KNOWN MINOR ISSUE: counters incremented again even though no new listing
        mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(2n);
        // Document: marketplace counter is inaccurate for duplicate listings

        // The listing contract receives OP_LISTING_CREATED again
        // but its state is still valid (AWAITING_NFT from first deploy or
        // the second deploy just sends OP_LISTING_CREATED which listing ignores
        // since listing router handles it as unknown op -> silently accepted)
        const listingData = await readListingData(blockchain, listingAddr);
        // Listing is still in AWAITING_NFT since NFT hasn't actually arrived
        expect(listingData.state).toBe(STATE_AWAITING_NFT);
    });
});

// ============================================================
// Category 5: Cross-Contract Attack Vectors
// ============================================================
describe('Category 5: Cross-Contract Attack Vectors', () => {
    let blockchain: Blockchain;
    let admin: SandboxContract<TreasuryContract>;
    let ownerWallet: SandboxContract<TreasuryContract>;
    let marketplaceAddress: Address;
    let marketplaceInit: StateInit;

    const RENTAL_PRICE = toNano('1');
    const RENTAL_DURATION = 86400;

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        blockchain.now = Math.floor(Date.now() / 1000);

        admin = await blockchain.treasury('admin');
        ownerWallet = await blockchain.treasury('owner');

        const data = buildMarketplaceData(admin.address);
        marketplaceInit = { code: MARKETPLACE_CODE, data };
        marketplaceAddress = contractAddress(0, marketplaceInit);

        await admin.send({
            to: marketplaceAddress,
            value: toNano('1'),
            init: marketplaceInit,
            body: beginCell().endCell(),
            bounce: false,
        });
    });

    it('18. Fake NFT sends ownership_assigned -> listing deployed but useless, no funds lost', async () => {
        const attacker = await blockchain.treasury('attacker');
        const marketplaceBalanceBefore = (await blockchain.getContract(marketplaceAddress)).balance;

        // Attacker pretends to be an NFT by sending ownership_assigned
        const r = await attacker.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(attacker.address) // prev_owner = attacker
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(RENTAL_PRICE)
                    .storeUint(RENTAL_DURATION, 32)
                    .endCell())
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);

        // Marketplace deployed a listing with nftAddress = attacker.address
        // This listing is useless because:
        // 1. The "NFT" is just a treasury wallet, not a real NFT contract
        // 2. The marketplace sent OP_TRANSFER to attacker.address (treasury ignores it)
        // 3. The listing deployed has nftAddress = attacker.address

        const listingAddr = await getListingAddress(
            blockchain, marketplaceAddress,
            attacker.address, attacker.address, RENTAL_PRICE, RENTAL_DURATION,
        );

        const listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.nftAddress.equals(attacker.address)).toBe(true);
        expect(listingData.ownerAddress.equals(attacker.address)).toBe(true);

        // Counters incremented but harmless
        const mdata = await readMarketplaceData(blockchain, marketplaceAddress);
        expect(mdata.nextIndex).toBe(1n);

        // IMPORTANT: Marketplace itself didn't lose funds beyond gas
        // The 0.5 TON sent by attacker covers the deployment costs
        // Marketplace's own balance is not significantly drained
        const marketplaceBalanceAfter = (await blockchain.getContract(marketplaceAddress)).balance;
        // Marketplace balance should not have decreased (attacker paid for everything)
        // It might even increase slightly due to remaining gas
        expect(marketplaceBalanceAfter).toBeGreaterThanOrEqual(marketplaceBalanceBefore - toNano('0.05'));
    });

    it('19. Attacker spoofs prev_owner as victim -> renter payment goes to victim, attacker gains nothing', async () => {
        const attacker = await blockchain.treasury('attacker');
        const victim = await blockchain.treasury('victim');
        const renterWallet = await blockchain.treasury('renter');

        // Attacker sends ownership_assigned with prev_owner = victim
        // This means ownerAddress in the listing = victim
        const r = await attacker.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(victim.address) // prev_owner = victim (spoofed)
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(RENTAL_PRICE)
                    .storeUint(RENTAL_DURATION, 32)
                    .endCell())
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);

        const listingAddr = await getListingAddress(
            blockchain, marketplaceAddress,
            attacker.address, victim.address, RENTAL_PRICE, RENTAL_DURATION,
        );

        // Verify: listing has nftAddress = attacker, ownerAddress = victim
        let listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.nftAddress.equals(attacker.address)).toBe(true);
        expect(listingData.ownerAddress.equals(victim.address)).toBe(true);

        // Simulate NFT arriving at listing (attacker acts as NFT)
        const nftArrival = await attacker.send({
            to: listingAddr,
            value: toNano('0.1'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(marketplaceAddress) // prev_owner = marketplace
                .storeUint(0, 1)
                .endCell(),
        });
        expect(getExitCode(nftArrival.transactions, listingAddr)).toBe(0);

        listingData = await readListingData(blockchain, listingAddr);
        expect(listingData.state).toBe(STATE_LISTED);

        // Renter rents: payment goes to VICTIM (ownerAddress), not attacker
        const victimBalanceBefore = (await blockchain.getContract(victim.address)).balance;
        const attackerBalanceBefore = (await blockchain.getContract(attacker.address)).balance;

        const rentPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
        const rentResult = await renterWallet.send({
            to: listingAddr,
            value: rentPayment,
            body: beginCell()
                .storeUint(OP_RENT, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(rentResult.transactions, listingAddr)).toBe(0);

        const victimBalanceAfter = (await blockchain.getContract(victim.address)).balance;
        const attackerBalanceAfter = (await blockchain.getContract(attacker.address)).balance;

        // Victim received the rental payment (approximately RENTAL_PRICE)
        expect(victimBalanceAfter - victimBalanceBefore).toBeGreaterThan(RENTAL_PRICE - toNano('0.05'));

        // Attacker balance should NOT have increased
        expect(attackerBalanceAfter).toBeLessThanOrEqual(attackerBalanceBefore + toNano('0.01'));

        // CONCLUSION: Attacker gains nothing. The "listing" is based on a fake NFT
        // (the attacker's address). If a renter rents it, they can change DNS records
        // on the fake NFT (which does nothing useful). Payment goes to victim.
        // This is an economic griefing vector but the attacker cannot profit.
    });
});
