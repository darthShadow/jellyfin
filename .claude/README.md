# Jellyfin Project Documentation

Technical documentation and development guides for the Jellyfin project.

---

## Documentation Structure

### Current Implementation

**`database/`**
Documentation of the current production database implementation.

- **DB_OPERATIONAL_GUIDE.md** - Comprehensive operational guide
  - EFCore + SQLite architecture
  - Connection pooling and lifecycle management
  - SQLite pragma configuration
  - Three concurrency strategies (NoLock, Pessimistic, Optimistic)
  - Transaction management
  - Performance optimization
  - Backup and restore procedures

---

### Future Development

**`development/`**
Design documents and implementation guides for planned features.

**`development/database/`**
- **DUAL_POOL_DESIGN.md** - Dual reader/writer connection pool design
  - Complete architecture design
  - Implementation guide with code examples
  - Migration strategy (3 phases)
  - Performance analysis and testing

---

## Quick Links

### For Current Operations
→ See **`database/DB_OPERATIONAL_GUIDE.md`**

Use this to:
- Understand current database architecture
- Configure SQLite pragmas and locking behaviors
- Troubleshoot connection issues
- Optimize performance
- Perform backup and restore

### For Development Work
→ See **`development/database/DUAL_POOL_DESIGN.md`**

Use this to:
- Implement dual reader/writer pools
- Understand design decisions and alternatives
- Follow implementation guide
- Plan migration strategy
- Write tests

---

## Project Configuration

**`.claude/settings.local.json`**
Local settings for Claude Code development environment.

---

## Contributing Documentation

When adding new documentation:

1. **Current functionality** → `database/` or appropriate feature folder
2. **Future features** → `development/{feature}/`
3. **Update this README** with links to new documentation
4. **Keep separation clear** between current and planned features

---

## Summary

```
.claude/
├── README.md                                    ← You are here
├── settings.local.json                          ← Local settings
├── database/
│   └── DB_OPERATIONAL_GUIDE.md                 ← Current: Database operations
└── development/
    └── database/
        └── DUAL_POOL_DESIGN.md                 ← Future: Dual-pool architecture
```

**Current documentation**: Production reality
**Development documentation**: Planned enhancements
