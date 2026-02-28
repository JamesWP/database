DOT=dot

PROG=target/debug/database
PROG=target/release/database

# Pattern rules
%.svg : %.dot
	$(DOT) -Tsvg -o $@ $<

%.dot : %.db $(PROG)
	rm -f $@
	echo "enter btree\ndump $@" | $(PROG) $<

screenshots: $(PROG)
	rm -f doc/screenshots/demo.db doc/screenshots/demo-preloaded.db
	$(PROG) doc/screenshots/demo-preloaded.db sql "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
	$(PROG) doc/screenshots/demo-preloaded.db sql "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28), (5, 'eve', 22), (6, 'frank', 31)"
	python3 doc/screenshots/check.py
	cp doc/screenshots/demo-preloaded.db demo-preloaded.db
	rm -f demo.db
	PATH="$(PWD)/.bin:$(PATH)" vhs doc/screenshots/repl-sql.tape
	PATH="$(PWD)/.bin:$(PATH)" vhs doc/screenshots/repl-index.tape
	rm -f demo.db demo-preloaded.db doc/screenshots/demo.db doc/screenshots/demo-preloaded.db

big.db:
	rm -f $@
	echo "enter btree\ncreate table a\nread table a\nrandom insert 1000000 250" | $(PROG) $@