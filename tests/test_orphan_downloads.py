"""临时目录和模拟下载器验证插件的孤立文件清理，不连接 NAS 或数据库。"""
import ast
import datetime
import fcntl
import importlib.util
import json
import logging
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import Mock
import uuid
import pytest

PACKAGE=Path(__file__).resolve().parents[1]/'plugins.v3/unwatchedcleanup'
spec=importlib.util.spec_from_file_location('orphan_files', PACKAGE/'orphan_files.py')
files=importlib.util.module_from_spec(spec)
spec.loader.exec_module(files)
tree=ast.parse((PACKAGE/'__init__.py').read_text())
klass=next(n for n in tree.body if isinstance(n,ast.ClassDef))
namespace=dict(globals(), _PluginBase=object, BackgroundScheduler=object,
    logger=logging.getLogger('test'), lock=threading.Lock(), HISTORY_KEEP=300,
    scan_orphan_files=files.scan, purge_source=files.purge_source,
    human=lambda n:str(n), settings=SimpleNamespace(TZ='UTC'),
    CronTrigger=SimpleNamespace(from_crontab=lambda text,timezone:SimpleNamespace(cron=text,timezone=timezone)),
    DownloaderHelper=Mock(), time=SimpleNamespace(time=lambda:time.time()+172800))
exec(compile(ast.Module(body=[klass],type_ignores=[]),str(PACKAGE/'__init__.py'),'exec'),namespace)
Plugin=namespace['UnwatchedCleanup']

@pytest.fixture
def scenario(tmp_path):
    """创建一个孤立目录和一个下载器正在使用的目录。"""
    root=tmp_path/'download';library=tmp_path/'library'
    root.mkdir();library.mkdir()
    orphan=root/'orphan';(orphan/'nested').mkdir(parents=True)
    (orphan/'nested'/'episode.mkv').write_bytes(b'video')
    (orphan/'note.nfo').write_text('metadata')
    live=root/'live';live.mkdir();(live/'keep.mkv').write_bytes(b'active')
    client=Mock();client.get_torrents.return_value=([{'content_path':'/downloads/media/live'}],False)
    helper=Mock();helper.get_configs.return_value={'qB':object()}
    helper.get_services.return_value={'qB':SimpleNamespace(type='qbittorrent',instance=client)}
    config={'download_dirs':f'{root}:/downloads/media','library_dirs':[str(library)],'min_age_days':1}
    namespace['DownloaderHelper'].return_value=helper
    p=Plugin();p.init_plugin({'enabled':True,'dry_run':False,'notify':False,'library_dirs':str(library),
        'orphan_enabled':True,'orphan_dry_run':False,'orphan_download_dirs':config['download_dirs']})
    data={}
    p.get_data=lambda key:json.loads(json.dumps(data.get(key)))
    p.save_data=lambda key,value:data.update({key:json.loads(json.dumps(value))})
    p.get_data_path=lambda:tmp_path/'state'
    return SimpleNamespace(root=root,library=library,orphan=orphan,helper=helper,client=client,config=config,plugin=p,data=data)

def scan(s,now=None):
    """使用未来时间模拟文件已经静置两天。"""
    return files.scan(s.config,s.helper,now or time.time()+172800)

def test_nested_and_single_orphans_deleted_but_live_data_retained(scenario):
    s=scenario;(s.root/'single.mkv').write_bytes(b'single')
    assert s.plugin.run_orphans()['success']
    assert not s.orphan.exists() and not (s.root/'single.mkv').exists()
    assert (s.root/'live'/'keep.mkv').read_bytes()==b'active'
    assert s.data['orphan_last_scan']['deleted']==2
    assert all(x['status']=='success' for x in s.data['orphan_history'])

@pytest.mark.parametrize('mode',['endpoint','global','orphan'])
def test_preview_controls_never_delete(scenario,mode):
    s=scenario
    if mode=='global':s.plugin._dry_run=True
    if mode=='orphan':s.plugin._orphan_dry_run=True
    result=s.plugin.preview_orphans() if mode=='endpoint' else s.plugin.run_orphans()
    assert result['success'] and s.orphan.exists()
    assert s.data['orphan_last_scan']['deleted']==0
    assert not s.data.get('orphan_history')

def test_recent_file_retained(scenario):
    assert scan(scenario,now=time.time())['rows']==[]

@pytest.mark.parametrize('symlink',[False,True])
def test_media_library_references_protect_release(scenario,symlink):
    s=scenario;target=s.library/'reference.mkv';source=s.orphan/'nested'/'episode.mkv'
    if symlink:target.symlink_to(source)
    else:os.link(source,target)
    assert scan(s)['rows']==[]

def test_all_downloaders_including_paused_tasks_protected(scenario):
    s=scenario
    s.helper.get_configs.return_value['second']=object()
    second=Mock();second.get_torrents.return_value=([{'content_path':'/downloads/media/orphan','state':'pausedDL'}],False)
    s.helper.get_services.return_value['second']=SimpleNamespace(type='qbittorrent',instance=second)
    assert scan(s)['rows']==[]

@pytest.mark.parametrize('failure',['offline','disabled','missing_path','missing_library'])
def test_incomplete_inventory_or_library_aborts(scenario,failure):
    s=scenario
    if failure=='offline':s.client.get_torrents.return_value=(None,True)
    elif failure=='disabled':s.helper.get_configs.return_value['disabled']=object()
    elif failure=='missing_path':s.client.get_torrents.return_value=([{}],False)
    else:s.library.rmdir()
    assert not s.plugin.run_orphans()['success']
    assert s.orphan.exists() and not s.data.get('orphan_history')

def test_no_remaining_tasks_still_cleans_old_orphans(scenario):
    s=scenario;s.client.get_torrents.return_value=([],False)
    assert len(scan(s)['rows'])==2

def test_symlink_directories_and_mounts_retained(scenario,monkeypatch):
    s=scenario;(s.orphan/'link').symlink_to(s.library,target_is_directory=True)
    assert not scan(s)['rows']
    (s.orphan/'link').unlink()
    monkeypatch.setattr(files.os.path,'ismount',lambda path:Path(path)==s.orphan)
    assert not scan(s)['rows']

def test_library_symlink_directory_stops_scan(scenario):
    s=scenario;(s.library/'link').symlink_to(s.orphan,target_is_directory=True)
    with pytest.raises(ValueError):scan(s)

def test_old_partial_removed_only_after_task_disappears(scenario):
    s=scenario;(s.orphan/'part.!qB').write_bytes(b'partial')
    assert len(scan(s)['rows'])==1
    s.client.get_torrents.return_value[0].append({'content_path':'/downloads/media/orphan'})
    assert not scan(s)['rows']

@pytest.mark.parametrize('change',['content','new_hardlink','existing_hardlink','root_replaced'])
def test_delete_rechecks_changes_and_links(scenario,change):
    s=scenario;source=s.orphan/'note.nfo'
    if change=='existing_hardlink':os.link(source,s.root.parent/'outside')
    row=scan(s)['rows'][0]
    if change=='content':source.write_text('new content')
    elif change=='new_hardlink':os.link(source,s.library/'new-link')
    elif change=='root_replaced':
        moved=s.root.with_name('moved');s.root.rename(moved);s.root.symlink_to(moved,target_is_directory=True)
    with pytest.raises(ValueError):files.purge_source(row)
    assert (s.orphan/'nested'/'episode.mkv').exists()

def test_new_file_during_delete_is_never_removed(scenario,monkeypatch):
    s=scenario;row=scan(s)['rows'][0];original=files.os.unlink;new=s.orphan/'new-file'
    def concurrent_write(*args,**kwargs):
        new.write_text('new data');return original(*args,**kwargs)
    monkeypatch.setattr(files.os,'unlink',concurrent_write)
    with pytest.raises(OSError):files.purge_source(row)
    assert new.read_text()=='new data'

def test_new_task_before_action_preserves_files(scenario):
    s=scenario
    s.client.get_torrents.side_effect=[([{'content_path':'/downloads/media/live'}],False),([{'content_path':'/downloads/media/orphan'},{'content_path':'/downloads/media/live'}],False)]
    assert s.plugin.run_orphans()['success']
    assert s.orphan.exists() and s.data['orphan_last_scan']['deleted']==0

def test_partial_deletion_failure_retains_audit(scenario,monkeypatch):
    s=scenario
    def failure(*args,**kwargs):raise OSError('simulated')
    monkeypatch.setitem(namespace,'purge_source',failure)
    assert not s.plugin.run_orphans()['success']
    assert s.data['orphan_history'][0]['status']=='uncertain'
    assert s.orphan.exists()

def test_stop_configuration_and_operation_lock_prevent_delete(scenario):
    s=scenario;s.plugin._stop.set()
    assert not s.plugin.run_orphans()['success'] and s.orphan.exists()
    namespace['lock'].acquire()
    try:assert not s.plugin.run_orphans()['success']
    finally:namespace['lock'].release()

def test_both_jobs_registered_and_default_install_safe(scenario):
    jobs=scenario.plugin.get_service()
    assert {j['id'] for j in jobs}=={'UnwatchedCleanup','OrphanDownloads'}
    assert {j['trigger'].cron for j in jobs}=={'30 6 * * *','0 5 * * *'}
    new=Plugin();new.get_data=lambda key:None;new.init_plugin(None)
    assert not new.orphan_status()['enabled']
    assert new.orphan_status()['preview']
    assert new.get_service()==[]

def test_unsafe_directory_mappings_rejected(scenario):
    s=scenario;s.config['download_dirs']=str(s.library)+':/downloads/media'
    with pytest.raises(ValueError):scan(s)
    for mapping in ('/:/downloads/media','/media/download:/','/media/../download:/downloads/media','/media/download:/downloads/media\n/media/download/sub:/downloads/sub'):
        with pytest.raises(ValueError):files.parse_mappings(mapping)

def test_transmission_paths_preserve_actual_renamed_file(scenario):
    s=scenario;torrent=SimpleNamespace(download_dir='/downloads/media',get_files=lambda:[{'name':'orphan/nested/episode.mkv'}])
    client=SimpleNamespace(_trarg=[],trc=SimpleNamespace(get_torrents=lambda **kwargs:[torrent]))
    s.helper.get_services.return_value={'qB':SimpleNamespace(type='transmission',instance=client)}
    assert all(row['path']!=str(s.orphan) for row in scan(s)['rows'])
