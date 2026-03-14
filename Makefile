DOT=dot
SHELL=/bin/bash

PROG=target/release/database

VHS=PATH="$(PWD)/.bin:$(PATH)" vhs

# Pattern rules for dot/svg
%.svg: %.dot
	$(DOT) -Tsvg -o $@ $<

%.dot: %.db $(PROG)
	rm -f $@
	echo "enter btree\ndump $@" | $(PROG) $<

# Build targets
target/debug/database:
	cargo build

target/release/database:
	cargo build --release

# Test databases

# Preloaded users seed db — never mutated; tapes get a fresh copy each run
demo-preloaded.db.seed: $(PROG)
	rm -f $@
	$(PROG) $@ sql "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
	$(PROG) $@ sql "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28), (5, 'eve', 22), (6, 'frank', 31)"

demo.db: $(PROG)
	rm -f $@
	$(PROG) $@ sql "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
	$(PROG) $@ sql "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28), (5, 'eve', 22), (6, 'frank', 31), (7, 'grace', 27), (8, 'henry', 33), (9, 'iris', 29), (10, 'jack', 24), (11, 'kate', 38), (12, 'leo', 26), (13, 'mia', 31), (14, 'noah', 29), (15, 'olivia', 34), (16, 'paul', 23), (17, 'quinn', 36), (18, 'rose', 28), (19, 'sam', 41), (20, 'tara', 26), (21, 'uri', 32), (22, 'vera', 27), (23, 'will', 39), (24, 'xena', 25), (25, 'yusuf', 30), (26, 'zoe', 22), (27, 'adam', 45), (28, 'bella', 29), (29, 'carlos', 33), (30, 'diana', 37), (31, 'ethan', 28), (32, 'fiona', 24), (33, 'george', 42), (34, 'hana', 31), (35, 'ivan', 26), (36, 'julia', 35)"
	$(PROG) $@ sql "CREATE TABLE orders (order_id INTEGER, user_id INTEGER, product TEXT, amount INTEGER)"
	$(PROG) $@ sql "INSERT INTO orders VALUES (1, 1, 'laptop', 1200), (2, 1, 'mouse', 25), (3, 2, 'keyboard', 80), (4, 3, 'monitor', 400), (5, 3, 'hdmi cable', 15), (6, 4, 'webcam', 90), (7, 5, 'headphones', 150), (8, 6, 'desk lamp', 40), (9, 7, 'usb hub', 35), (10, 8, 'ssd', 120), (11, 9, 'charger', 30), (12, 10, 'mousepad', 20), (13, 11, 'speaker', 200), (14, 12, 'tablet', 350), (15, 13, 'phone stand', 18), (16, 14, 'cable ties', 8), (17, 15, 'monitor arm', 75), (18, 16, 'trackball', 60), (19, 17, 'drawing tablet', 280), (20, 18, 'nvme drive', 95)"
	$(PROG) $@ sql "INSERT INTO orders VALUES (21, 19, 'laptop bag', 55), (22, 20, 'docking station', 180), (23, 21, 'screen cleaner', 12), (24, 22, 'blue light glasses', 35), (25, 23, 'standing mat', 65), (26, 24, 'usb-c hub', 45), (27, 25, 'external gpu', 650), (28, 26, 'ram 32gb', 110), (29, 27, 'capture card', 160), (30, 28, 'webcam ring light', 40), (31, 29, 'ergonomic chair', 520), (32, 30, 'desk mat', 30), (33, 31, 'microphone', 230), (34, 32, 'mixer', 190), (35, 33, 'stream deck', 150), (36, 34, 'vr headset', 900), (37, 35, 'keyboard wrist rest', 22), (38, 36, 'monitor light bar', 50), (39, 1, 'second monitor', 380), (40, 3, 'mechanical keyboard', 140)"

big.db: $(PROG)
	rm -f $@
	echo "enter btree\ncreate table a\nread table a\nrandom insert 1000000 250" | $(PROG) $@

# Screenshot GIFs + TXT
# Tapes that need no preloaded db (tape creates its own)
doc/screenshots/%.gif doc/screenshots/%.txt &: doc/screenshots/%.tape $(PROG)
	$(VHS) $<

# repl-sql creates demo.db during the session - ensure it starts fresh
doc/screenshots/repl-sql.gif doc/screenshots/repl-sql.txt &: doc/screenshots/repl-sql.tape $(PROG)
	rm -f demo.db
	$(VHS) $<

# Tapes that require a preloaded db — copy seed fresh each run so tape mutations don't persist
doc/screenshots/repl-index.gif doc/screenshots/repl-index.txt &: doc/screenshots/repl-index.tape $(PROG) demo-preloaded.db.seed
	cp demo-preloaded.db.seed demo-preloaded.db
	$(VHS) $<

doc/screenshots/repl-engine.gif doc/screenshots/repl-engine.txt &: doc/screenshots/repl-engine.tape $(PROG) demo-preloaded.db.seed
	cp demo-preloaded.db.seed demo-preloaded.db
	$(VHS) $<

NORMALIZE=python3 doc/screenshots/normalize.py

# Verify txt output against committed expected files; stamp avoids re-checking if nothing changed
doc/screenshots/%.checked: doc/screenshots/%.txt doc/screenshots/%.expected doc/screenshots/normalize.py
	diff <($(NORMALIZE) < $<) <($(NORMALIZE) < $(word 2,$^))
	touch $@

# Update committed expected files from current txt output (stores normalized form)
.PHONY: update-screenshots
update-screenshots: doc/screenshots/repl-sql.txt doc/screenshots/repl-index.txt doc/screenshots/repl-engine.txt
	$(NORMALIZE) < doc/screenshots/repl-sql.txt > doc/screenshots/repl-sql.expected
	$(NORMALIZE) < doc/screenshots/repl-index.txt > doc/screenshots/repl-index.expected
	$(NORMALIZE) < doc/screenshots/repl-engine.txt > doc/screenshots/repl-engine.expected

SCREENSHOTS = \
	doc/screenshots/repl-sql.gif \
	doc/screenshots/repl-index.gif \
	doc/screenshots/repl-engine.gif \
	doc/screenshots/repl-debug.gif

CHECKS = \
	doc/screenshots/repl-sql.checked \
	doc/screenshots/repl-index.checked \
	doc/screenshots/repl-engine.checked

screenshots: $(CHECKS) $(SCREENSHOTS)
