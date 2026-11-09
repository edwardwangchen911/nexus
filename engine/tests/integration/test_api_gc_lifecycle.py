"""The CLI freeze must not retain later HTTP/session cycles or cancellation state."""

import json
import os
import subprocess
from pathlib import Path

import pytest
from helpers.paper_batch import seed_paper_batch


@pytest.mark.asyncio
async def test_cli_gc_preserves_normal_failed_and_cancelled_request_reclamation(
    migrated_database_url,
):
    await seed_paper_batch(migrated_database_url)
    source = r"""
import asyncio,gc,json,os,resource,weakref
from datetime import datetime,UTC
import httpx
from sqlalchemy import event
from sqlalchemy.orm import Session
from helpers.paper_api import app as fixture_app,headers
from nexus import main
from nexus.workbench.read_service import WorkbenchReadService
refs=[]
def ended(session,transaction):
 if transaction.parent is None:
  refs.extend([weakref.ref(session),weakref.ref(transaction)])
event.listen(Session,'after_transaction_end',ended)
main.create_engine_app=lambda:fixture_app(os.environ['DATABASE_URL'],now=datetime(2026,8,27,14,1,tzinfo=UTC))
threshold=gc.get_threshold();initial_freeze=gc.get_freeze_count();result={}
class Cycle:pass
async def requests(app):
 path='/internal/v1/workbench'
 async with httpx.AsyncClient(
  transport=httpx.ASGITransport(app=app),base_url='http://fixture'
 ) as client:
  for _ in range(6):
   response=await client.get(path,headers=headers('GET',path))
   assert response.status_code==200 and response.json()['data']['execution_locked']
   refs.extend([weakref.ref(response),weakref.ref(response.request)])
   await response.aclose();del response
  bad=path+'?forbidden=1'
  response=await client.get(bad,headers=headers('GET',bad));assert response.status_code==422
  await response.aclose();del response
  entered=asyncio.Event();wait=asyncio.Event();native=WorkbenchReadService.read
  async def paused(self):
   entered.set();await wait.wait()
  WorkbenchReadService.read=paused
  try:
   task=asyncio.create_task(client.get(path,headers=headers('GET',path)))
   await entered.wait();task.cancel()
   try:await task
   except asyncio.CancelledError:pass
   del task
  finally:WorkbenchReadService.read=native
  await asyncio.sleep(0)
 await app.state.session_factory.kw['bind'].dispose()
def serve(app,**kwargs):
 assert gc.isenabled() and gc.get_threshold()==threshold
 before=gc.get_freeze_count();assert before>initial_freeze
 result['rss_before_KiB']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
 asyncio.run(requests(app))
 item=Cycle();item.self=item;reference=weakref.ref(item);del item
 gc.collect()
 assert reference() is None
 assert all(ref() is None for ref in refs)
 assert gc.get_freeze_count()<=before
 result.update(reclaimed=True,gc_enabled=gc.isenabled(),threshold_unchanged=gc.get_threshold()==threshold,rss_after_KiB=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
main.uvicorn.run=serve
main.run()
print(json.dumps(result))
"""
    root = Path(__file__).resolve().parents[3]
    environment = {
        **os.environ,
        "DATABASE_URL": migrated_database_url,
        "PYTHONPATH": os.pathsep.join([str(root / "engine/src"), str(root / "engine/tests")]),
    }
    process = subprocess.run(
        [str(root / ".venv/bin/python"), "-B", "-"],
        input=source,
        text=True,
        capture_output=True,
        env=environment,
        timeout=45,
        cwd=root,
    )
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout.splitlines()[-1])
    assert result["reclaimed"] and result["gc_enabled"] and result["threshold_unchanged"]
