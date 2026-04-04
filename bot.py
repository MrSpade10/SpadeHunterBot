# bot.py'ı tamamen temizle - tüm ters slashları forward slash'a çevir
$content = Get-Content bot.py -Encoding UTF8 -Raw
$cleaned = $content.Replace('\U', '/U').Replace('\', '/').Replace('"c:', '"c/').Replace('PowerShell', '')
$cleaned | Set-Content bot.py -Encoding UTF8

# Commit et
git add bot.py
git commit -m "Fix: Clean bot.py unicode escape sequences"
git push origin main
git add bot.py
git commit -m "Fix: Clean bot.py unicode escape sequences"
git push origin main
