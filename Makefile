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
	$(PROG) $@ sql "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28), (5, 'eve', 22), (6, 'frank', 31), (7, 'grace', 27), (8, 'henry', 33), (9, 'iris', 29), (10, 'jack', 24), (11, 'kate', 38), (12, 'leo', 26), (13, 'mia', 31), (14, 'noah', 29), (15, 'olivia', 34), (16, 'paul', 23), (17, 'quinn', 36), (18, 'rose', 28), (19, 'sam', 41), (20, 'tara', 26), (21, 'uri', 32), (22, 'vera', 27), (23, 'will', 39), (24, 'xena', 25), (25, 'yusuf', 30), (26, 'zoe', 22), (27, 'adam', 45), (28, 'bella', 29), (29, 'carlos', 33), (30, 'diana', 37), (31, 'ethan', 28), (32, 'fiona', 24), (33, 'george', 42), (34, 'hana', 31), (35, 'ivan', 26), (36, 'julia', 35)"
	$(PROG) $@ sql "CREATE TABLE orders (order_id INTEGER, user_id INTEGER, product TEXT, amount INTEGER)"
	$(PROG) $@ sql "INSERT INTO orders VALUES (1, 1, 'laptop', 1200), (2, 1, 'mouse', 25), (3, 2, 'keyboard', 80), (4, 3, 'monitor', 400), (5, 3, 'hdmi cable', 15), (6, 4, 'webcam', 90), (7, 5, 'headphones', 150), (8, 6, 'desk lamp', 40), (9, 7, 'usb hub', 35), (10, 8, 'ssd', 120), (11, 9, 'charger', 30), (12, 10, 'mousepad', 20), (13, 11, 'speaker', 200), (14, 12, 'tablet', 350), (15, 13, 'phone stand', 18), (16, 14, 'cable ties', 8), (17, 15, 'monitor arm', 75), (18, 16, 'trackball', 60), (19, 17, 'drawing tablet', 280), (20, 18, 'nvme drive', 95)"
	$(PROG) $@ sql "INSERT INTO orders VALUES (21, 19, 'laptop bag', 55), (22, 20, 'docking station', 180), (23, 21, 'screen cleaner', 12), (24, 22, 'blue light glasses', 35), (25, 23, 'standing mat', 65), (26, 24, 'usb-c hub', 45), (27, 25, 'external gpu', 650), (28, 26, 'ram 32gb', 110), (29, 27, 'capture card', 160), (30, 28, 'webcam ring light', 40), (31, 29, 'ergonomic chair', 520), (32, 30, 'desk mat', 30), (33, 31, 'microphone', 230), (34, 32, 'mixer', 190), (35, 33, 'stream deck', 150), (36, 34, 'vr headset', 900), (37, 35, 'keyboard wrist rest', 22), (38, 36, 'monitor light bar', 50), (39, 1, 'second monitor', 380), (40, 3, 'mechanical keyboard', 140)"

big.db:
	rm -f $@
	echo "enter btree\ncreate table a\nread table a\nrandom insert 1000000 250" | $(PROG) $@