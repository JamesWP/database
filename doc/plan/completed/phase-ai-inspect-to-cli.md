# Phase AI — Move `inspect_page` to CLI

Move the `BTree::inspect_page` diagnostic method out of the library and into the
btree REPL mode in `crates/database-cli`, eliminating the only use of `colored`
in `src/storage/btree.rs` and keeping diagnostic presentation entirely in the CLI.

This phase supersedes **AH item 125** ("remove colored from btree.rs inspect —
plain text output"). If AI is completed, AH item 125 can be skipped.

## Items

| # | Track | Item | Depends on |
|---|-------|------|------------|
| 127 | 6 | Add `BTree::pager()` accessor; re-implement `inspect_page` in CLI btree REPL mode | Phase AG |
| 128 | 6 | Remove `BTree::inspect_page` and `colored` from `src/storage/btree.rs` | 127 |

---
Important: Each item should be committed separately, follow 'Git Workflow' in CLAUDE.md

---

## Overview

`BTree::inspect_page` is a diagnostic tool — it dumps the raw on-disk structure
of a page for debugging serialization, overflow chains, and CBOR encoding.  It
has no place in a library API: it calls `println!` directly and depends on
`colored` for terminal colouring.

All the types it needs are already `pub`:
- `pager::Pager` — `get_file_size_pages()`, `get_and_decode()`
- `pager::ZeroPage`
- `node::NodePage`, `LeafNodePage`, `InteriorNodePage`, `OverflowPage`

The only missing piece is access to `BTree`'s private `pager` field.  Adding a
`pub fn pager(&self) -> Ref<'_, Pager>` accessor is sufficient — the CLI's btree
REPL mode can then drive the inspection itself.

```
Before:  CLI btree mode  →  BTree::inspect_page()  →  println!(colored output)
After:   CLI btree mode  →  BTree::pager()          →  formats + println!(colored output)
```

The behavior visible to the user (`cargo run -p database-cli -- test.db btree inspect page 0`)
is identical.  The library is cleaner.

---

## 127. Add `BTree::pager()` accessor; re-implement inspect in CLI (Track 6)

### What Changes

**`src/storage/btree.rs`**: Add a single new public method:

```rust
use std::cell::Ref;
use super::pager::Pager;

impl BTree {
    /// Borrow the underlying Pager for diagnostic / inspection purposes.
    pub fn pager(&self) -> Ref<'_, Pager> {
        self.pager.borrow()
    }
}
```

**`crates/database-cli/src/repl/modes/btree.rs`**: Replace the two calls to
`shared.btree.inspect_page(page_num)` with a local `inspect_page` free function
(or method on the btree mode state) that takes `&BTree` and does everything
`BTree::inspect_page` currently does — including the colored output:

```rust
use colored::Colorize as _;
use database::storage::{
    btree::BTree,
    node::{NodePage, OverflowPage},
    pager::ZeroPage,
};

fn inspect_page(btree: &BTree, page_num: u32) -> Result<(), String> {
    let pager = btree.pager();
    let file_size = pager.get_file_size_pages();

    if page_num >= file_size {
        return Err(format!(
            "Page {} out of range (file has {} pages)",
            page_num, file_size
        ));
    }

    println!(
        "{}",
        format!("Page {} raw CBOR structure:", page_num)
            .bright_cyan()
            .bold()
    );
    println!("{}", "=====================================".bright_black());

    if page_num == 0 {
        let zero: ZeroPage = pager.get_and_decode(0);
        println!("{}: {}", "Type".yellow(), "ZeroPage".green());
        println!("{:#?}", zero);
    } else {
        let node: NodePage = pager.get_and_decode(page_num);
        // ... rest of the match arms, identical to current BTree::inspect_page ...
    }

    Ok(())
}
```

The two call sites in the btree mode become:

```rust
// Before
match shared.btree.inspect_page(page_num) { ... }

// After
match inspect_page(&shared.btree, page_num) { ... }
```

The colored output and all formatting is preserved exactly — the function body is
copied verbatim from `BTree::inspect_page`; only the location changes.

### Key Files

- `src/storage/btree.rs` — add `pub fn pager(&self) -> Ref<'_, Pager>`
- `crates/database-cli/src/repl/modes/btree.rs` — add `inspect_page` free function, update call sites

### Tests

```bash
cargo build --workspace
cargo run -p database-cli -- test.db btree inspect page 0
cargo run -p database-cli -- test.db btree inspect all
# Output must be identical to before
```

### Implementation Steps (1 commit)

#### Step 127.1 — Add pager() accessor; move inspect_page to CLI btree mode

**Commit:** `Refactor: move inspect_page to CLI btree mode, expose BTree::pager() accessor`

---

## 128. Remove `BTree::inspect_page` and `colored` from `btree.rs` (Track 6)

### What Changes

1. **`src/storage/btree.rs`**: Delete the `inspect_page` method entirely.  Remove
   `use colored::Colorize;` from the file.
2. Verify no other code in the library references `colored`.

After this commit, `src/storage/btree.rs` has zero `colored` usage.  Combined
with the `colored` removals from `scalarvalue.rs` and `program.rs` in Phase AH,
the library has no `colored` dependency at all (enabling AH item 126 — dropping
`colored` from `Cargo.toml`).

### Key Files

- `src/storage/btree.rs` — delete `inspect_page`, remove `use colored::Colorize`

### Tests

```bash
cargo build -p database        # library builds without colored
cargo build --workspace        # full build
cargo test --workspace
# btree inspect still works (CLI implementation from item 127)
cargo run -p database-cli -- test.db btree inspect page 0
```

### Implementation Steps (1 commit)

#### Step 128.1 — Delete BTree::inspect_page; remove colored from btree.rs

**Commit:** `Refactor: remove BTree::inspect_page and colored from storage layer`

---

## Verification

- [ ] `cargo build --workspace` — clean, zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `grep -r "colored" src/` — empty (no colored in library source)
- [ ] `cargo tree -p database | grep colored` — empty
- [ ] `cargo run -p database-cli -- test.db btree inspect page 0` — coloured output identical to before
- [ ] `cargo run -p database-cli -- test.db btree inspect all` — works correctly
- [ ] Each commit is independently buildable
