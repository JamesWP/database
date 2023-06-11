DOT=dot

# Pattern rules
%.svg : %.dot
	$(DOT) -Tsvg -o $@ $<

%.dot : %.db target/debug/database
	echo "dump $@" | target/debug/database $<