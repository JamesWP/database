DOT=dot

PROG=target/debug/database
PROG=target/release/database

# Pattern rules
%.svg : %.dot
	$(DOT) -Tsvg -o $@ $<

%.dot : %.db $(PROG)
	rm -f $@
	echo "enter btree\ndump $@" | $(PROG) $<

big.db:
	rm -f $@
	echo "enter btree\ncreate table a\nread table a\nrandom insert 1000000 250" | $(PROG) $@