<p align="center"><img src="banner.png" alt="TON DNS Lease V1" /></p>

<p align="center">TON DNS Lease V1 - Smart contracts for leasing TON DNS domain NFTs. Owner lists a domain with a price and duration. Renter pays, controls DNS records for the lease period, then the NFT returns to the owner.</p>

<p align="center">
  <a href="https://github.com/TONresistor/ton-dns-lease/actions/workflows/ci.yml"><img src="https://github.com/TONresistor/ton-dns-lease/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/TON-Blockchain-0088CC?style=flat&logo=ton&logoColor=white" alt="TON" />
  <img src="https://img.shields.io/badge/Tolk-1.2-blue?style=flat" alt="Tolk 1.2" />
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat" alt="MIT License" />
  <a href="https://t.me/ResistanceTools"><img src="https://img.shields.io/badge/Telegram-@ResistanceTools-26A5E4?style=flat&logo=telegram&logoColor=white" alt="Telegram" /></a>
</p>

## How It Works

1. **List**:Owner transfers DNS NFT to the Marketplace with `rental_price` and `rental_duration` in the forward payload. Marketplace deploys a Listing and forwards the NFT to it.
2. **Rent**:Anyone sends `OP_RENT` + payment. Listing forwards payment to owner, starts the timer.
3. **Use**:Renter sends `OP_CHANGE_RECORD` to update DNS records. Listing proxies to the NFT.
4. **Renew**:Renter extends with `OP_RENEW`. Owner can block renewals with `OP_STOP_RENEWAL`.
5. **Return**:After expiry, anyone calls `OP_CLAIM_BACK`. NFT goes back to owner.

## Contracts

### Marketplace (`dns-rent-marketplace.tolk`)

Factory. Receives NFT via `ownership_assigned`, deploys a Listing, forwards NFT.

| Get method | Returns |
|------------|---------|
| `get_marketplace_data()` | `(owner_address, next_listing_index)` |
| `get_listing_address(nft, owner, price, duration)` | deterministic Listing address |

### Listing (`dns-rent-listing.tolk`)

Escrow. Holds one NFT, enforces lease terms.

States: `AWAITING_NFT(0)` > `LISTED(1)` > `RENTED(2)` > `CLOSED(3)`

| Get method | Returns |
|------------|---------|
| `get_listing_data()` | full listing state |
| `get_rental_status()` | `(state, rental_end_time, remaining_seconds)` |

## Operations

| Opcode | Name | Caller | State | Effect |
|--------|------|--------|-------|--------|
| `0x05138d91` | ownership_assigned | NFT | AWAITING_NFT | NFT received, LISTED |
| `0x52454e54` | rent | Anyone | LISTED | Pays owner, RENTED |
| `0x4368526b` | change_record | Renter | RENTED | Proxies DNS record update |
| `0x52456e77` | renew | Renter | RENTED | Extends lease |
| `0x53745270` | stop_renewal | Owner | RENTED | Blocks future renewals |
| `0x436c4261` | claim_back | Anyone | RENTED (expired) | NFT to owner, CLOSED |
| `0x44654c73` | delist | Owner | LISTED | NFT to owner, CLOSED |
| `0x57746864` | withdraw_excess | Owner | Any | Withdraws excess TON |
| `0x52654c73` | relist | Owner | CLOSED | Resets to AWAITING_NFT |
| `0x456d5274` | emergency_return | Mkt. owner | Marketplace | Returns stuck NFT |

## Error Codes

| Code | Name | Meaning |
|------|------|---------|
| 100 | ERR_NOT_OWNER | Not the listing owner |
| 101 | ERR_NFT_NOT_RECEIVED | NFT not transferred yet |
| 102 | ERR_NFT_ALREADY_RECEIVED | NFT already received |
| 103 | ERR_WRONG_NFT | Wrong NFT contract |
| 105 | ERR_LISTING_NOT_ACTIVE | Wrong state |
| 111 | ERR_INSUFFICIENT_PAYMENT | Payment too low |
| 112 | ERR_NOT_RENTED | No active rental |
| 113 | ERR_RENTAL_EXPIRED | Lease ended |
| 114 | ERR_RENTAL_NOT_EXPIRED | Lease still active |
| 115 | ERR_OVERFLOW | uint32 overflow |
| 116 | ERR_RENEWAL_DISABLED | Renewals blocked |
| 117 | ERR_INSUFFICIENT_BALANCE | Balance too low |
| 120 | ERR_NOT_RENTER | Not the renter |
| 125 | ERR_FORBIDDEN_OP | Blocked opcode |
| 130 | ERR_ACTIVE_RENTAL | Can't delist while rented |
| 140 | ERR_NOT_MARKETPLACE_OWNER | Not marketplace owner |
| 142 | ERR_ZERO_PRICE | Price below 0.15 TON |
| 143 | ERR_ZERO_DURATION | Duration is zero |

## Security

- NFT held by Listing contract, not by renter or owner, for the entire lease
- Direct `transfer` and `change_dns_record` opcodes blocked at router level
- Sender validated on every privileged operation
- State machine enforced: invalid transitions revert
- Bounce recovery: failed NFT transfer reverts CLOSED to LISTED (sender-verified)
- Renewal capped at 1 year, duration capped at 1 year, price floor 0.15 TON
- Balance check before NFT transfer prevents action-phase lock
- Gas reserved after every operation for eventual claim_back

## Build and Test

```
npm install
npm run build
npm test
```

141 tests across 3 suites: lifecycle, listing security, marketplace security.

## Structure

```
contracts/
  dns-rent-listing.tolk        Listing (escrow, lease logic)
  dns-rent-marketplace.tolk    Marketplace (factory)
  messages.tolk                Opcodes, constants
  errors.tolk                  Error codes
  schema.tlb                   TL-B schema
tests/
  dns-rent.test.ts             Lifecycle tests
  security-audit.test.ts       Listing exploits
  marketplace-audit.test.ts    Marketplace exploits
```
