# Plan: Introduce a Schema Catalog Table

This document outlines the plan to refactor the database to use a "schema catalog" table. This change moves the responsibility of tracking tables and their physical locations out of the low-level `Pager` and into a high-level, queryable table.

The primary goal is to enable the query planner to find table metadata (specifically, the B-Tree root page number) by querying this new catalog. This is a foundational step before implementing DDL statements like `CREATE TABLE`.

## 1. Catalog Table Definition

- A special, hard-coded table named `db_schema` will be created.
- It will have a fixed schema:
  - `type`: `TEXT` (e.g., 'table', 'index')
  - `name`: `TEXT` (The name of the object, e.g., 'users')
  - `tbl_name`: `TEXT` (The name of the table this object belongs to)
  - `rootpage`: `INTEGER` (The page number of the B-Tree root for this object)
  - `sql`: `TEXT` (The DDL statement used to create this object, e.g., `CREATE TABLE ...`)
- The root page for the `db_schema` table itself will be a fixed, well-known page number (e.g., page 1).

## 2. Database Bootstrapping

- The `BTree::new()` function (or equivalent database constructor) will be modified.
- When a new database file is created, it will no longer be empty. It will be "bootstrapped" with the `db_schema` table.
- This process involves:
  1. Manually creating the B-Tree for the `db_schema` table at its well-known root page.
  2. Manually adding rows to this B-Tree to describe itself and any other built-in or test tables. For example, to make the old `"test"` table available, we will add a row to `db_schema` like:
     `('table', 'test', 'test', <rootpage_for_test_table>, 'CREATE TABLE test (col1, col2);')`

## 3. Remove Name-based Lookups from Pager

- The following methods, which rely on string names, will be removed from `src/storage/pager.rs`:
  - `get_root_page(tree_name: &str)`
  - `set_root_page(tree_name: &str, page_idx: u32)`
- The `BTree::open(tree_name: &str)` method will be changed to `BTree::open(root_page: u32)`, accepting a page number directly.
- The `BTree::create_tree` method will be removed for now, as table creation will eventually be handled by the engine, not directly in the storage layer.

## 4. Refactor Query Execution Flow

This is the core of the change.

- **Planner Modification:**
  - When the planner receives a query for a table (e.g., `SELECT name, age FROM users`), it will first generate and execute a *meta-query* against the `db_schema` table.
  - The meta-query will now be: `SELECT rootpage, sql FROM db_schema WHERE name = 'users'`.
  - This meta-query will be executed using a simplified path that knows the hard-coded location (`rootpage = 1`) and schema of `db_schema`.
  - **DDL Parsing:** The planner will take the `sql` string (e.g., `"CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"`) returned from the meta-query. It will use the existing SQL frontend (parser and lexer) to parse this string into a `CreateTableStatement` AST. From this AST, the planner will build a "schema object" for the `users` table, containing a mapping of column names to their index (e.g., `{"id": 0, "name": 1, "age": 2}`).
  - **Plan Generation:** The planner will use the `rootpage` from the meta-query and the column mapping from the parsed DDL to generate the final plan. For a `SELECT`, it will map the requested column names to their indices and create a `LogicalPlan::Scan` containing those indices.

- **Compiler & Engine Modification:**
  - The `LogicalPlan::Scan` enum will be changed from `{ table: String, columns: Vec<usize> }` to `{ rootpage: u32, columns: Vec<usize> }`.
  - The `codegen_scan` function will be updated to accept a `rootpage` instead of a `table` string. The `num_columns` parameter will be derived from the length of the `columns` vector.
  - The `Operation::Open` bytecode instruction will be changed to take a `u32` root page number instead of a `String` table name.
  - The `Engine`'s handler for `Operation::Open` will now receive a page number and pass it directly to a new `BTree::open(rootpage: u32)` method.

## 5. Update Tests

- All tests that currently use `btree.create_tree("test")` will be refactored.
- The test setup will now involve bootstrapping the database with a `db_schema` table that already contains the definition for the `"test"` table. This ensures the tests can run against the new catalog-based lookup mechanism.
