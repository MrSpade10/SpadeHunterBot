Copy-Item "c:\Users\salih\OneDrive\Masaüstü\Claude\agents" . -Recurse -Force
Copy-Item "c:\Users\salih\OneDrive\Masaüstü\Claude\ruflo.config.json" . -Force
git add .
git commit -m "Add 10 Ruflo agents"
git push
