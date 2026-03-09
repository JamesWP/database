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
	PATH="$(PWD)/.bin:$(PATH)" vhs doc/screenshots/repl-engine.tape
	rm -f demo.db demo-preloaded.db doc/screenshots/demo.db doc/screenshots/demo-preloaded.db

demo.db: $(PROG)
	rm -f $@
	$(PROG) $@ sql "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
	$(PROG) $@ sql "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28), (5, 'eve', 22), (6, 'frank', 31), (7, 'grace', 27), (8, 'henry', 33), (9, 'iris', 29), (10, 'jack', 24), (11, 'kate', 38), (12, 'leo', 26)"
	$(PROG) $@ sql "CREATE TABLE orders (order_id INTEGER, user_id INTEGER, product TEXT, amount INTEGER)"
	$(PROG) $@ sql "INSERT INTO orders VALUES (1, 1, 'laptop', 1200), (2, 1, 'mouse', 25), (3, 2, 'keyboard', 80), (4, 3, 'monitor', 400), (5, 3, 'hdmi cable', 15), (6, 4, 'webcam', 90), (7, 5, 'headphones', 150), (8, 6, 'desk lamp', 40), (9, 7, 'usb hub', 35), (10, 8, 'ssd', 120), (11, 9, 'charger', 30), (12, 10, 'mousepad', 20), (13, 11, 'speaker', 200), (14, 12, 'tablet', 350)"

big.db:
	rm -f $@
	echo "enter btree\ncreate table a\nread table a\nrandom insert 1000000 250" | $(PROG) $@