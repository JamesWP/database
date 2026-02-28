Output doc/screenshots/repl-index.gif

Set FontSize 14
Set Width 900
Set Height 400
Set Theme "Catppuccin Mocha"
Env PATH "/home/james/gits/database/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

Type "db demo-preloaded.db"
Enter
Sleep 1500ms

Type "enter sql"
Enter
Sleep 400ms

Type "SELECT * FROM users WHERE age = 30 ORDER BY age"
Enter
Sleep 400ms

Type "EXPLAIN SELECT * FROM users WHERE age = 30 ORDER BY age"
Enter
Sleep 1000ms

Type "CREATE INDEX idx_age ON users (age)"
Enter
Sleep 500ms

Type "EXPLAIN SELECT * FROM users WHERE age = 30 ORDER BY age"
Enter
Sleep 8s
