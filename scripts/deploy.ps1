Write-Host "== Heimdall deploy checklist ==" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Push your changes:" -ForegroundColor Yellow
Write-Host "   git add -A; git commit -m `"...`"; git push"
Write-Host ""
Write-Host "2. SSH into TrueNAS, then run:" -ForegroundColor Yellow
Write-Host "   cd /mnt/<your-pool>/apps/heimdall"
Write-Host "   git pull"
Write-Host "   docker compose build"
Write-Host "   docker compose up -d"
Write-Host ""
Write-Host "3. Watch both services start cleanly:" -ForegroundColor Yellow
Write-Host "   docker compose logs -f heimdall-server heimdall-ui"
Write-Host ""
Write-Host "4. First deploy only - pull the model:" -ForegroundColor Yellow
Write-Host "   docker exec heimdall-ollama ollama pull qwen2.5:0.5b"
Write-Host ""
Write-Host "5. Confirm the split actually works - restart the worker and watch" -ForegroundColor Yellow
Write-Host "   the dashboard stay responsive the whole time:"
Write-Host "   (do this from the Ops tab in the UI, or: docker compose restart heimdall-server)"