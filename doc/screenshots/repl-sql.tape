Output doc/screenshots/repl-sql.gif
Output doc/screenshots/repl-sql.txt

Set FontSize 14
Set Width 900
Set Height 520
Set Theme "Catppuccin Mocha"
Env PATH "/home/james/gits/database/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

Type "db demo.db"
Enter
Sleep 1500ms

Type "enter sql"
Enter
Sleep 400ms

Type "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
Enter
Sleep 500ms

Type "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), (4, 'dave', 28)"
Enter
Sleep 400ms

Type "SELECT * FROM users WHERE age > 27 ORDER BY name"
Enter
Sleep 5s
