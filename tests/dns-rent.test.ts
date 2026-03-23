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
            return tx.description?.aborted === true;
        }
    }
    return false;
}

// Storage layout: main cell + rental ref
// Main: marketplaceAddr + nftAddress + ownerAddress + state(uint8) + nftReceived(int1) + ref(rentalRef)
// RentalRef: renterAddress(addr_none/address) + rentalPrice(coins) + rentalDuration(uint32) + rentalEndTime(uint32) + renewalAllowed(int1)
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

function buildMarketplaceData(ownerAddr: Address, listingCode: Cell): Cell {
    return beginCell()
        .storeAddress(ownerAddr)
        .storeRef(listingCode)
        .storeUint(0, 64)   // next_listing_index
        .endCell();
}

// ============================================================
// Marketplace Tests
// ============================================================
describe('Marketplace', () => {
    let blockchain: Blockchain;
    let admin: SandboxContract<TreasuryContract>;
    let user: SandboxContract<TreasuryContract>;
    let marketplaceAddress: Address;
    let marketplaceInit: StateInit;

    const RENTAL_PRICE = toNano('1');
    const RENTAL_DURATION = 86400;

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        admin = await blockchain.treasury('admin');
        user = await blockchain.treasury('user');

        const data = buildMarketplaceData(admin.address, LISTING_CODE);
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

    // Helper: simulate NFT sending ownership_assigned to marketplace
    async function sendNftToMarketplace(
        nftWallet: SandboxContract<TreasuryContract>,
        ownerWallet: SandboxContract<TreasuryContract>,
        price: bigint = RENTAL_PRICE,
        duration: number = RENTAL_DURATION,
    ) {
        return nftWallet.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)                    // query_id
                .storeAddress(ownerWallet.address)   // prev_owner = real owner
                .storeUint(1, 1)                     // forward_payload in ref
                .storeRef(beginCell()
                    .storeCoins(price)
                    .storeUint(duration, 32)
                    .endCell())
                .endCell(),
        });
    }

    // Helper: get listing address from marketplace get method
    async function getListingAddress(
        nftAddr: Address,
        ownerAddr: Address,
        price: bigint = RENTAL_PRICE,
        duration: number = RENTAL_DURATION,
    ): Promise<Address> {
        const provider = blockchain.provider(marketplaceAddress);
        const { stack } = await provider.get('get_listing_address', [
            { type: 'slice', cell: beginCell().storeAddress(nftAddr).endCell() },
            { type: 'slice', cell: beginCell().storeAddress(ownerAddr).endCell() },
            { type: 'int', value: price },
            { type: 'int', value: BigInt(duration) },
        ]);
        // get_listing_address returns a slice, read as cell then parse address
        const addrCell = stack.readCell();
        const addrSlice = addrCell.beginParse();
        return addrSlice.loadAddress();
    }

    it('should deploy marketplace successfully', async () => {
        const provider = blockchain.provider(marketplaceAddress);
        const { stack } = await provider.get('get_marketplace_data', []);
        const ownerAddr = stack.readAddress();
        const nextIndex = stack.readBigNumber();

        expect(ownerAddr.equals(admin.address)).toBe(true);
        expect(nextIndex).toBe(0n);
    });

    it('should create listing via NFT transfer (ownership_assigned)', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user);

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
        expect(isAborted(r.transactions, marketplaceAddress)).toBe(false);

        // Verify marketplace state updated
        const provider = blockchain.provider(marketplaceAddress);
        const { stack } = await provider.get('get_marketplace_data', []);
        stack.readAddress(); // owner
        const nextIndex = stack.readBigNumber();
        expect(nextIndex).toBe(1n);
    });

    it('should deploy listing contract via ownership_assigned', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user);

        // Should have multiple transactions (marketplace + listing deploy + NFT transfer + notification)
        expect(r.transactions.length).toBeGreaterThanOrEqual(3);
    });

    it('should send listing_created notification to owner', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await nftWallet.send({
            to: marketplaceAddress,
            value: toNano('0.5'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(42, 64)                   // query_id = 42
                .storeAddress(user.address)          // prev_owner
                .storeUint(1, 1)
                .storeRef(beginCell()
                    .storeCoins(RENTAL_PRICE)
                    .storeUint(RENTAL_DURATION, 32)
                    .endCell())
                .endCell(),
        });

        // Find notification message sent to user (prev_owner)
        const userMsgs = r.transactions.filter(
            (tx: any) =>
                tx.inMessage?.info?.type === 'internal' &&
                tx.inMessage?.info?.dest?.equals?.(user.address)
        );
        expect(userMsgs.length).toBeGreaterThan(0);

        // Verify notification body
        const body = userMsgs[0].inMessage?.body;
        if (body) {
            const bs = body.beginParse();
            expect(bs.loadUint(32)).toBe(OP_LISTING_CREATED);
            expect(bs.loadUint(64)).toBe(42); // query_id preserved
        }
    });

    it('should send NFT transfer to listing address', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user);

        // A transfer message should be sent back to the NFT (nftWallet)
        const nftMsgs = r.transactions.filter(
            (tx: any) => tx.inMessage?.info?.dest?.equals?.(nftWallet.address)
        );
        expect(nftMsgs.length).toBeGreaterThan(0);

        // Find the message containing OP_TRANSFER
        let foundTransfer = false;
        for (const tx of nftMsgs) {
            const msgBody = tx.inMessage?.body;
            if (msgBody) {
                const bs = msgBody.beginParse();
                let bodySlice = bs;
                if (bs.remainingBits < 32 && bs.remainingRefs > 0) {
                    bodySlice = bs.loadRef().beginParse();
                }
                if (bodySlice.remainingBits >= 32) {
                    const op = bodySlice.loadUint(32);
                    if (op === OP_TRANSFER) {
                        foundTransfer = true;
                        break;
                    }
                }
            }
        }
        expect(foundTransfer).toBe(true);
    });

    it('should reject listing with zero price', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user, 0n);

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_ZERO_PRICE);
    });

    it('should reject listing with price below MIN_TONS_FOR_STORAGE', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user, toNano('0.01'));

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_ZERO_PRICE);
    });

    it('should reject listing with zero duration', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user, RENTAL_PRICE, 0);

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_ZERO_DURATION);
    });

    it('should reject listing with duration > 1 year', async () => {
        const nftWallet = await blockchain.treasury('nft');

        const r = await sendNftToMarketplace(nftWallet, user, RENTAL_PRICE, 31536001);

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_INVALID_PARAMS);
    });

    it('should reject unknown opcodes', async () => {
        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('0.05'),
            body: beginCell()
                .storeUint(0xdeadbeef, 32)
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0xffff);
    });

    it('should accept empty messages (storage funding)', async () => {
        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('1'),
            body: beginCell().endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
    });

    it('should accept comment messages (op=0)', async () => {
        const r = await user.send({
            to: marketplaceAddress,
            value: toNano('0.05'),
            body: beginCell()
                .storeUint(0, 32)
                .storeStringTail('hello')
                .endCell(),
        });

        expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);
    });

    it('get_marketplace_data returns correct values', async () => {
        const provider = blockchain.provider(marketplaceAddress);
        const { stack } = await provider.get('get_marketplace_data', []);
        const ownerAddr = stack.readAddress();
        const nextIndex = stack.readBigNumber();

        expect(ownerAddr.equals(admin.address)).toBe(true);
        expect(nextIndex).toBe(0n);
    });

    it('get_listing_address returns deterministic address', async () => {
        const nftWallet = await blockchain.treasury('nft');
        const provider = blockchain.provider(marketplaceAddress);

        const args = [
            { type: 'slice' as const, cell: beginCell().storeAddress(nftWallet.address).endCell() },
            { type: 'slice' as const, cell: beginCell().storeAddress(user.address).endCell() },
            { type: 'int' as const, value: toNano('1') },
            { type: 'int' as const, value: 86400n },
        ];

        const call1 = await provider.get('get_listing_address', args);
        const addr1 = call1.stack.readCell();

        const call2 = await provider.get('get_listing_address', args);
        const addr2 = call2.stack.readCell();

        expect(addr1.equals(addr2)).toBe(true);
    });

    it('get_listing_address changes with different params', async () => {
        const nftWallet = await blockchain.treasury('nft');
        const provider = blockchain.provider(marketplaceAddress);

        const call1 = await provider.get('get_listing_address', [
            { type: 'slice', cell: beginCell().storeAddress(nftWallet.address).endCell() },
            { type: 'slice', cell: beginCell().storeAddress(user.address).endCell() },
            { type: 'int', value: toNano('1') },
            { type: 'int', value: 86400n },
        ]);
        const addr1 = call1.stack.readCell();

        const call2 = await provider.get('get_listing_address', [
            { type: 'slice', cell: beginCell().storeAddress(nftWallet.address).endCell() },
            { type: 'slice', cell: beginCell().storeAddress(user.address).endCell() },
            { type: 'int', value: toNano('2') },  // different price
            { type: 'int', value: 86400n },
        ]);
        const addr2 = call2.stack.readCell();

        expect(addr1.equals(addr2)).toBe(false);
    });

    // ============================================================
    // Emergency Return
    // ============================================================
    describe('Emergency Return', () => {
        it('marketplace owner can emergency return NFT', async () => {
            const nftWallet = await blockchain.treasury('nft');
            const returnTo = await blockchain.treasury('return-to');

            const r = await admin.send({
                to: marketplaceAddress,
                value: toNano('0.3'),
                body: beginCell()
                    .storeUint(OP_EMERGENCY_RETURN, 32)
                    .storeUint(0, 64)
                    .storeAddress(nftWallet.address)
                    .storeAddress(returnTo.address)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, marketplaceAddress)).toBe(0);

            // Should send OP_TRANSFER to the NFT contract
            const nftMsgs = r.transactions.filter(
                (tx: any) => tx.inMessage?.info?.dest?.equals?.(nftWallet.address)
            );
            expect(nftMsgs.length).toBeGreaterThan(0);

            const msgBody = nftMsgs[0].inMessage?.body;
            if (msgBody) {
                const bs = msgBody.beginParse();
                let bodySlice = bs;
                if (bs.remainingBits < 32 && bs.remainingRefs > 0) {
                    bodySlice = bs.loadRef().beginParse();
                }
                expect(bodySlice.loadUint(32)).toBe(OP_TRANSFER);
            }
        });

        it('non-owner cannot emergency return', async () => {
            const nftWallet = await blockchain.treasury('nft');
            const returnTo = await blockchain.treasury('return-to');

            const r = await user.send({
                to: marketplaceAddress,
                value: toNano('0.3'),
                body: beginCell()
                    .storeUint(OP_EMERGENCY_RETURN, 32)
                    .storeUint(0, 64)
                    .storeAddress(nftWallet.address)
                    .storeAddress(returnTo.address)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, marketplaceAddress)).toBe(ERR_NOT_MARKETPLACE_OWNER);
        });
    });
});

// ============================================================
// Listing Tests
// ============================================================
describe('Listing', () => {
    let blockchain: Blockchain;
    let ownerWallet: SandboxContract<TreasuryContract>;
    let nftWallet: SandboxContract<TreasuryContract>;
    let renterWallet: SandboxContract<TreasuryContract>;
    let marketplaceWallet: SandboxContract<TreasuryContract>;
    let listingAddress: Address;
    let listingInit: StateInit;

    const RENTAL_PRICE = toNano('1');
    const RENTAL_DURATION = 86400; // 1 day

    beforeEach(async () => {
        blockchain = await Blockchain.create();
        blockchain.now = Math.floor(Date.now() / 1000);

        ownerWallet = await blockchain.treasury('owner');
        nftWallet = await blockchain.treasury('nft');
        renterWallet = await blockchain.treasury('renter');
        marketplaceWallet = await blockchain.treasury('marketplace');

        const data = buildListingData(
            marketplaceWallet.address,
            nftWallet.address,
            ownerWallet.address,
            RENTAL_PRICE,
            RENTAL_DURATION,
        );

        listingInit = { code: LISTING_CODE, data };
        listingAddress = contractAddress(0, listingInit);

        // Deploy listing
        await ownerWallet.send({
            to: listingAddress,
            value: toNano('1'),
            init: listingInit,
            body: beginCell().endCell(),
            bounce: false,
        });
    });

    // Helper: simulate NFT sending ownership_assigned to listing
    async function sendOwnershipAssigned(
        fromNft: SandboxContract<TreasuryContract>,
        prevOwner: Address,
    ) {
        return fromNft.send({
            to: listingAddress,
            value: toNano('0.1'),
            body: beginCell()
                .storeUint(OP_OWNERSHIP_ASSIGNED, 32)
                .storeUint(0, 64)
                .storeAddress(prevOwner)
                .storeUint(0, 1)
                .endCell(),
        });
    }

    // Helper: transition listing to LISTED state
    async function transitionToListed() {
        const r = await sendOwnershipAssigned(nftWallet, ownerWallet.address);
        expect(getExitCode(r.transactions, listingAddress)).toBe(0);
    }

    // Helper: transition listing to RENTED state
    async function transitionToRented() {
        await transitionToListed();
        const totalPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
        const r = await renterWallet.send({
            to: listingAddress,
            value: totalPayment,
            body: beginCell()
                .storeUint(OP_RENT, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(r.transactions, listingAddress)).toBe(0);
    }

    // Helper: transition to CLOSED state (rent -> expire -> claim back)
    async function transitionToClosed() {
        await transitionToRented();
        blockchain.now = blockchain.now! + RENTAL_DURATION + 100;
        const r = await ownerWallet.send({
            to: listingAddress,
            value: toNano('0.2'),
            body: beginCell()
                .storeUint(OP_CLAIM_BACK, 32)
                .storeUint(0, 64)
                .endCell(),
        });
        expect(getExitCode(r.transactions, listingAddress)).toBe(0);
    }

    // ============================================================
    // NFT Reception
    // ============================================================
    describe('NFT Reception', () => {
        it('should accept NFT from correct address', async () => {
            const r = await sendOwnershipAssigned(nftWallet, ownerWallet.address);
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            const state = stack.readNumber();
            expect(state).toBe(STATE_LISTED);
        });

        it('should accept NFT from marketplace (no prev_owner check)', async () => {
            // NFT can arrive from marketplace, not just owner directly
            const r = await sendOwnershipAssigned(nftWallet, marketplaceWallet.address);
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_LISTED);
        });

        it('should set nft_received flag', async () => {
            await transitionToListed();

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            stack.readNumber();     // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            stack.readAddressOpt(); // renter (null)
            stack.readBigNumber();  // price
            stack.readBigNumber();  // duration
            stack.readBigNumber();  // end_time
            const nftReceived = stack.readNumber();
            expect(nftReceived).toBe(-1); // true in Tolk
        });

        it('should reject NFT from wrong address', async () => {
            const fakeNft = await blockchain.treasury('fake-nft');
            const r = await sendOwnershipAssigned(fakeNft, ownerWallet.address);
            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_WRONG_NFT);
        });

        it('should reject NFT if already received', async () => {
            await transitionToListed();
            const r = await sendOwnershipAssigned(nftWallet, ownerWallet.address);
            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NFT_ALREADY_RECEIVED);
        });

        it('should send excess to owner after accepting NFT', async () => {
            const r = await sendOwnershipAssigned(nftWallet, ownerWallet.address);
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const ownerMsgs = r.transactions.filter(
                (tx: any) => tx.inMessage?.info?.dest?.equals?.(ownerWallet.address)
            );
            expect(ownerMsgs.length).toBeGreaterThan(0);
        });
    });

    // ============================================================
    // Renting
    // ============================================================
    describe('Renting', () => {
        it('should accept rental with sufficient payment', async () => {
            await transitionToListed();

            const totalPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
            const r = await renterWallet.send({
                to: listingAddress,
                value: totalPayment,
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            // Verify state changed to RENTED
            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_RENTED);
        });

        it('should set renter address correctly', async () => {
            await transitionToRented();

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            stack.readNumber();     // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            const renter = stack.readAddressOpt();
            expect(renter).not.toBeNull();
            expect(renter!.equals(renterWallet.address)).toBe(true);
        });

        it('should forward payment to owner', async () => {
            await transitionToListed();

            const ownerBalanceBefore = (await blockchain.getContract(ownerWallet.address)).balance;

            const totalPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
            await renterWallet.send({
                to: listingAddress,
                value: totalPayment,
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const ownerBalanceAfter = (await blockchain.getContract(ownerWallet.address)).balance;
            expect(ownerBalanceAfter - ownerBalanceBefore).toBeGreaterThan(RENTAL_PRICE - toNano('0.01'));
        });

        it('should set correct rental end time', async () => {
            await transitionToListed();

            const now = blockchain.now!;
            const totalPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
            await renterWallet.send({
                to: listingAddress,
                value: totalPayment,
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            stack.readNumber();     // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            stack.readAddressOpt(); // renter
            stack.readBigNumber();  // price
            stack.readBigNumber();  // duration
            const endTime = stack.readBigNumber();
            expect(Number(endTime)).toBeGreaterThanOrEqual(now + RENTAL_DURATION);
        });

        it('should reject rental if not listed', async () => {
            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('2'),
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_LISTING_NOT_ACTIVE);
        });

        it('should reject rental with insufficient payment', async () => {
            await transitionToListed();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.01'),
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_INSUFFICIENT_PAYMENT);
        });

        it('should reject rental if already rented', async () => {
            await transitionToRented();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('2'),
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_LISTING_NOT_ACTIVE);
        });
    });

    // ============================================================
    // Record Changes
    // ============================================================
    describe('Record Changes', () => {
        it('renter can change DNS records', async () => {
            await transitionToRented();

            const recordKey = 0n;
            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_CHANGE_RECORD, 32)
                    .storeUint(0, 64)
                    .storeUint(recordKey, 256)
                    .storeUint(0, 1)  // no record_value ref
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            // Verify a message was sent to the NFT contract with OP_CHANGE_DNS_RECORD
            const outMsgs = r.transactions.filter(
                (tx: any) => tx.inMessage?.info?.dest?.equals?.(nftWallet.address)
            );
            expect(outMsgs.length).toBeGreaterThan(0);
        });

        it('non-renter cannot change records', async () => {
            await transitionToRented();

            const otherUser = await blockchain.treasury('other');
            const r = await otherUser.send({
                to: listingAddress,
                value: toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_CHANGE_RECORD, 32)
                    .storeUint(0, 64)
                    .storeUint(0, 256)
                    .storeUint(0, 1)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_RENTER);
        });

        it('cannot change records after expiration', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_CHANGE_RECORD, 32)
                    .storeUint(0, 64)
                    .storeUint(0, 256)
                    .storeUint(0, 1)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_RENTAL_EXPIRED);
        });

        it('cannot change records when not rented', async () => {
            await transitionToListed();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_CHANGE_RECORD, 32)
                    .storeUint(0, 64)
                    .storeUint(0, 256)
                    .storeUint(0, 1)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_RENTED);
        });
    });

    // ============================================================
    // Claim Back
    // ============================================================
    describe('Claim Back', () => {
        it('anyone can claim back after expiration', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            const anyUser = await blockchain.treasury('anyone');
            const r = await anyUser.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_CLOSED);
        });

        it('cannot claim back before expiration', async () => {
            await transitionToRented();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_RENTAL_NOT_EXPIRED);
        });

        it('claim back sets state to CLOSED', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_CLOSED);
        });

        it('claim back transfers NFT to owner', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // Verify NFT transfer message sent to NFT contract
            const nftMsgs = r.transactions.filter(
                (tx: any) => tx.inMessage?.info?.dest?.equals?.(nftWallet.address)
            );
            expect(nftMsgs.length).toBeGreaterThan(0);

            const msgBody = nftMsgs[0].inMessage?.body;
            if (msgBody) {
                const bs = msgBody.beginParse();
                let bodySlice = bs;
                if (bs.remainingBits < 32 && bs.remainingRefs > 0) {
                    bodySlice = bs.loadRef().beginParse();
                }
                expect(bodySlice.loadUint(32)).toBe(OP_TRANSFER);
            }
        });

        it('claim back clears renter address', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            stack.readNumber();     // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            const renter = stack.readAddressOpt();
            expect(renter).toBeNull();
        });

        it('cannot claim back when not rented', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_RENTED);
        });
    });

    // ============================================================
    // Delist
    // ============================================================
    describe('Delist', () => {
        it('owner can delist when listed', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_CLOSED);
        });

        it('non-owner cannot delist', async () => {
            await transitionToListed();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_OWNER);
        });

        it('cannot delist when in AWAITING_NFT state', async () => {
            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_ACTIVE_RENTAL);
        });

        it('cannot delist when rented', async () => {
            await transitionToRented();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_ACTIVE_RENTAL);
        });

        it('delist transfers NFT back to owner', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const nftMsgs = r.transactions.filter(
                (tx: any) => tx.inMessage?.info?.dest?.equals?.(nftWallet.address)
            );
            expect(nftMsgs.length).toBeGreaterThan(0);
        });

        it('delist sends NFT transfer with correct query_id and new_owner', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(42, 64)  // query_id = 42
                    .endCell(),
            });

            const nftMsgs = r.transactions.filter(
                (tx: any) => tx.inMessage?.info?.dest?.equals?.(nftWallet.address)
            );
            expect(nftMsgs.length).toBeGreaterThan(0);

            const msgCell = nftMsgs[0].inMessage?.body;
            if (msgCell) {
                const bs = msgCell.beginParse();
                let bodySlice = bs;
                if (bs.remainingBits < 32 && bs.remainingRefs > 0) {
                    bodySlice = bs.loadRef().beginParse();
                }
                expect(bodySlice.loadUint(32)).toBe(OP_TRANSFER);
                expect(bodySlice.loadUint(64)).toBe(42);
                const newOwner = bodySlice.loadAddress();
                expect(newOwner.equals(ownerWallet.address)).toBe(true);
            }
        });

        it('cannot delist twice', async () => {
            await transitionToListed();

            const r1 = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });
            expect(getExitCode(r1.transactions, listingAddress)).toBe(0);

            const r2 = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r2.transactions, listingAddress)).toBe(ERR_ACTIVE_RENTAL);
        });
    });

    // ============================================================
    // Renewal
    // ============================================================
    describe('Renewal', () => {
        it('renter can renew before expiration', async () => {
            await transitionToRented();

            const r = await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });

        it('non-renter cannot renew', async () => {
            await transitionToRented();

            const otherUser = await blockchain.treasury('other');
            const r = await otherUser.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_RENTER);
        });

        it('cannot renew after expiration', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            const r = await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_RENTAL_EXPIRED);
        });

        it('cannot renew with insufficient payment', async () => {
            await transitionToRented();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.01'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_INSUFFICIENT_PAYMENT);
        });

        it('renewal extends end time by duration', async () => {
            await transitionToRented();

            const provider = blockchain.provider(listingAddress);
            const { stack: s1 } = await provider.get('get_rental_status', []);
            s1.readNumber(); // state
            const endTimeBefore = s1.readBigNumber();

            await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const { stack: s2 } = await provider.get('get_rental_status', []);
            s2.readNumber();
            const endTimeAfter = s2.readBigNumber();

            expect(endTimeAfter).toBe(endTimeBefore + BigInt(RENTAL_DURATION));
        });

        it('renewal forwards payment to owner', async () => {
            await transitionToRented();

            const ownerBalanceBefore = (await blockchain.getContract(ownerWallet.address)).balance;

            await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const ownerBalanceAfter = (await blockchain.getContract(ownerWallet.address)).balance;
            expect(ownerBalanceAfter - ownerBalanceBefore).toBeGreaterThan(RENTAL_PRICE - toNano('0.01'));
        });

        it('cannot renew when not rented', async () => {
            await transitionToListed();

            const r = await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_RENTED);
        });

        it('multiple renewals extend time correctly', async () => {
            await transitionToRented();

            const provider = blockchain.provider(listingAddress);

            // First renewal
            await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const { stack: s1 } = await provider.get('get_rental_status', []);
            s1.readNumber();
            const endAfterFirst = s1.readBigNumber();

            // Second renewal
            await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const { stack: s2 } = await provider.get('get_rental_status', []);
            s2.readNumber();
            const endAfterSecond = s2.readBigNumber();

            expect(endAfterSecond).toBe(endAfterFirst + BigInt(RENTAL_DURATION));
        });
    });

    // ============================================================
    // Stop Renewal
    // ============================================================
    describe('Stop Renewal', () => {
        it('owner can stop renewals when rented', async () => {
            await transitionToRented();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_STOP_RENEWAL, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            // Verify renewalAllowed is now 0
            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            stack.readNumber();     // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            stack.readAddressOpt(); // renter
            stack.readBigNumber();  // price
            stack.readBigNumber();  // duration
            stack.readBigNumber();  // end_time
            stack.readNumber();     // nft_received
            const renewalAllowed = stack.readNumber();
            expect(renewalAllowed).toBe(0);
        });

        it('renter cannot renew after stop', async () => {
            await transitionToRented();

            // Owner stops renewals
            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_STOP_RENEWAL, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // Renter tries to renew
            const r = await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_RENEWAL_DISABLED);
        });

        it('non-owner cannot stop renewals', async () => {
            await transitionToRented();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_STOP_RENEWAL, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_OWNER);
        });

        it('cannot stop renewals when not rented', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_STOP_RENEWAL, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_RENTED);
        });

        it('relist resets renewal allowed', async () => {
            await transitionToRented();

            // Stop renewals
            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_STOP_RENEWAL, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // Expire and claim back
            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;
            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // Relist
            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_RELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // Verify renewalAllowed is reset to -1 (true)
            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            stack.readNumber();     // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            stack.readAddressOpt(); // renter
            stack.readBigNumber();  // price
            stack.readBigNumber();  // duration
            stack.readBigNumber();  // end_time
            stack.readNumber();     // nft_received
            const renewalAllowed = stack.readNumber();
            expect(renewalAllowed).toBe(-1); // true = reset

            // Now re-receive NFT and rent again, renewals should work
            await sendOwnershipAssigned(nftWallet, ownerWallet.address);

            const totalPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
            await renterWallet.send({
                to: listingAddress,
                value: totalPayment,
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // Renew should succeed
            const r = await renterWallet.send({
                to: listingAddress,
                value: RENTAL_PRICE + toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_RENEW, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });
    });

    // ============================================================
    // Relist
    // ============================================================
    describe('Relist', () => {
        it('owner can relist after CLOSED', async () => {
            await transitionToClosed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_RELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_AWAITING_NFT);
        });

        it('non-owner cannot relist', async () => {
            await transitionToClosed();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_RELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_OWNER);
        });

        it('cannot relist when not closed', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_RELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_LISTING_NOT_ACTIVE);
        });

        it('relist resets state fields', async () => {
            await transitionToClosed();

            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_RELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_AWAITING_NFT); // state
            stack.readAddress();    // nft
            stack.readAddress();    // owner
            const renter = stack.readAddressOpt();
            expect(renter).toBeNull(); // renter cleared
            stack.readBigNumber();  // price
            stack.readBigNumber();  // duration
            expect(stack.readBigNumber()).toBe(0n); // end_time reset
            expect(stack.readNumber()).toBe(0);     // nft_received = false
            expect(stack.readNumber()).toBe(-1);    // renewal_allowed reset to true
        });
    });

    // ============================================================
    // Withdraw Excess
    // ============================================================
    describe('Withdraw Excess', () => {
        it('owner can withdraw excess', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_WITHDRAW_EXCESS, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });

        it('non-owner cannot withdraw excess', async () => {
            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(OP_WITHDRAW_EXCESS, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_NOT_OWNER);
        });
    });

    // ============================================================
    // Security
    // ============================================================
    describe('Security', () => {
        it('rejects direct OP_TRANSFER', async () => {
            await transitionToRented();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_TRANSFER, 32)
                    .storeUint(0, 64)
                    .storeAddress(renterWallet.address)
                    .storeAddress(renterWallet.address)
                    .storeUint(0, 1)
                    .storeCoins(0)
                    .storeUint(0, 1)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_FORBIDDEN_OP);
        });

        it('rejects direct OP_CHANGE_DNS_RECORD', async () => {
            await transitionToRented();

            const r = await renterWallet.send({
                to: listingAddress,
                value: toNano('0.1'),
                body: beginCell()
                    .storeUint(OP_CHANGE_DNS_RECORD, 32)
                    .storeUint(0, 64)
                    .storeUint(0, 256)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(ERR_FORBIDDEN_OP);
        });
    });

    // ============================================================
    // GET Methods
    // ============================================================
    describe('GET Methods', () => {
        it('get_listing_data returns correct values in AWAITING_NFT state', async () => {
            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);

            expect(stack.readNumber()).toBe(STATE_AWAITING_NFT);
            expect(stack.readAddress().equals(nftWallet.address)).toBe(true);
            expect(stack.readAddress().equals(ownerWallet.address)).toBe(true);
        });

        it('get_listing_data returns all 9 values in LISTED state', async () => {
            await transitionToListed();

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);

            expect(stack.readNumber()).toBe(STATE_LISTED);
            expect(stack.readAddress().equals(nftWallet.address)).toBe(true);
            expect(stack.readAddress().equals(ownerWallet.address)).toBe(true);

            const renter = stack.readAddressOpt();
            expect(renter).toBeNull();

            expect(stack.readBigNumber()).toBe(RENTAL_PRICE);
            expect(stack.readBigNumber()).toBe(BigInt(RENTAL_DURATION));
            expect(stack.readBigNumber()).toBe(0n); // end_time
            expect(stack.readNumber()).toBe(-1);     // nft_received = true
            expect(stack.readNumber()).toBe(-1);     // renewal_allowed = true
        });

        it('get_listing_data returns correct values in RENTED state', async () => {
            await transitionToRented();

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);

            expect(stack.readNumber()).toBe(STATE_RENTED);
            stack.readAddress(); // nft
            stack.readAddress(); // owner
            const renter = stack.readAddressOpt();
            expect(renter).not.toBeNull();
            expect(renter!.equals(renterWallet.address)).toBe(true);

            expect(stack.readBigNumber()).toBe(RENTAL_PRICE);
            expect(stack.readBigNumber()).toBe(BigInt(RENTAL_DURATION));

            const endTime = stack.readBigNumber();
            expect(endTime).toBeGreaterThan(0n);

            expect(stack.readNumber()).toBe(-1); // nft_received
            expect(stack.readNumber()).toBe(-1); // renewal_allowed (still true)
        });

        it('get_listing_data returns correct values in CLOSED state', async () => {
            await transitionToListed();

            await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_CLOSED);
        });

        it('get_rental_status returns correct time remaining when rented', async () => {
            await transitionToRented();

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_rental_status', []);

            expect(stack.readNumber()).toBe(STATE_RENTED);

            const endTime = stack.readBigNumber();
            expect(endTime).toBeGreaterThan(0n);

            const remaining = stack.readBigNumber();
            expect(remaining).toBeGreaterThan(0n);
            expect(remaining).toBeLessThanOrEqual(BigInt(RENTAL_DURATION));
        });

        it('get_rental_status returns 0 remaining when not rented', async () => {
            await transitionToListed();

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_rental_status', []);

            expect(stack.readNumber()).toBe(STATE_LISTED);
            stack.readBigNumber(); // end_time
            expect(stack.readBigNumber()).toBe(0n); // remaining
        });

        it('get_rental_status returns 0 remaining when expired', async () => {
            await transitionToRented();

            blockchain.now = blockchain.now! + RENTAL_DURATION + 1000;

            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_rental_status', []);

            expect(stack.readNumber()).toBe(STATE_RENTED);
            stack.readBigNumber(); // end_time
            expect(stack.readBigNumber()).toBe(0n); // remaining clamped to 0
        });

        it('get_rental_status returns state and 0 remaining in AWAITING_NFT', async () => {
            const provider = blockchain.provider(listingAddress);
            const { stack } = await provider.get('get_rental_status', []);

            expect(stack.readNumber()).toBe(STATE_AWAITING_NFT);
            expect(stack.readBigNumber()).toBe(0n); // end_time
            expect(stack.readBigNumber()).toBe(0n); // remaining
        });
    });

    // ============================================================
    // Edge Cases
    // ============================================================
    describe('Edge Cases', () => {
        it('accepts empty messages (storage funding)', async () => {
            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('1'),
                body: beginCell().endCell(),
            });
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });

        it('accepts comment messages (op=0)', async () => {
            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(0, 32)
                    .storeStringTail('hello')
                    .endCell(),
            });
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });

        it('silently accepts unknown opcodes', async () => {
            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.05'),
                body: beginCell()
                    .storeUint(0xdeadbeef, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });

        it('owner can delist immediately after listing', async () => {
            await transitionToListed();

            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_DELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            expect(getExitCode(r.transactions, listingAddress)).toBe(0);
        });

        it('full lifecycle: list → rent → expire → claim back → relist', async () => {
            // 1. List
            await transitionToListed();

            // 2. Rent
            const totalPayment = RENTAL_PRICE + GAS_CHANGE_RECORD + toNano('0.1');
            await renterWallet.send({
                to: listingAddress,
                value: totalPayment,
                body: beginCell()
                    .storeUint(OP_RENT, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });

            // 3. Expire
            blockchain.now = blockchain.now! + RENTAL_DURATION + 100;

            // 4. Claim back
            const r = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_CLAIM_BACK, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });
            expect(getExitCode(r.transactions, listingAddress)).toBe(0);

            const provider = blockchain.provider(listingAddress);
            let { stack } = await provider.get('get_listing_data', []);
            expect(stack.readNumber()).toBe(STATE_CLOSED);

            // 5. Relist
            const r2 = await ownerWallet.send({
                to: listingAddress,
                value: toNano('0.2'),
                body: beginCell()
                    .storeUint(OP_RELIST, 32)
                    .storeUint(0, 64)
                    .endCell(),
            });
            expect(getExitCode(r2.transactions, listingAddress)).toBe(0);

            ({ stack } = await provider.get('get_listing_data', []));
            expect(stack.readNumber()).toBe(STATE_AWAITING_NFT);
        });
    });
});
