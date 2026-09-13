Write-Host "== Heimdall deploy ==" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Push your changes:" -ForegroundColor Yellow
Write-Host "   git add -A; git commit -m `"...`"; git push"
Write-Host ""
Write-Host "2. SSH into TrueNAS and hard-rebuild in one shot:" -ForegroundColor Yellow
Write-Host "   ssh truenas"
Write-Host "   cd /mnt/Apps/apps/heimdall"
Write-Host "   ./scripts/redeploy.sh          # or: make redeploy"
Write-Host ""
Write-Host "   This does a real git pull, tears down containers/orphans, clears" -ForegroundColor DarkGray
Write-Host "   build cache, rebuilds with --no-cache --pull, and brings the" -ForegroundColor DarkGray
Write-Host "   stack back up. Your database and pulled Ollama models are kept." -ForegroundColor DarkGray
Write-Host ""
Write-Host "3. Watch it come up:" -ForegroundColor Yellow
Write-Host "   docker compose logs -f worker controller"
Write-Host ""
Write-Host "4. First deploy only - pull the model:" -ForegroundColor Yellow
Write-Host "   docker exec heimdall-ollama ollama pull qwen2.5:0.5b"