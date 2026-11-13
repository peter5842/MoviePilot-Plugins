"""使用临时媒体目录和模拟服务验证清理行为。"""
import ast
import collections
import datetime
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple
import time
from unittest.mock import Mock
import pytest

SOURCE=Path(__file__).resolve().parents[1]/'plugins.v3/unwatchedcleanup/__init__.py'
tree=ast.parse(SOURCE.read_text())
klass=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='UnwatchedCleanup')
namespace=dict(globals(),_PluginBase=object,BackgroundScheduler=object,logger=logging.getLogger('test'),HISTORY_KEEP=300,VIDEO_EXT={'.mkv','.mp4'},human=lambda n:str(n),fmt_date=lambda n:str(n),DownloaderHelper=Mock())
exec(compile(ast.Module(body=[klass],type_ignores=[]),str(SOURCE),'exec'),namespace)
Plugin=namespace['UnwatchedCleanup']

@pytest.fixture
def scenario(tmp_path):
    """构造一个超过 180 天、宽限期已满的临时媒体条目。"""
    library=tmp_path/'tv'
    directory=library/'Show'
    directory.mkdir(parents=True)
    dest=directory/'S01E01.mkv'
    dest.write_bytes(b'video')
    source=tmp_path/'download.mkv'
    source.write_bytes(b'video')
    p=Plugin()
    p._library_dirs=[str(library)+'/']
    p._local_path=str(tmp_path)+'/'
    p._notify=False
    p._days=180
    p._grace_days=7
    p._dry_run=False
    item=dict(kind='tv',rating_key='show',title='Show',year='2020',tmdb='123',files=[str(dest)],keys={'show','season','episode'},newest_added=time.time()-200*86400,last_viewed=0,section='2')
    records={str(dest):dict(id=1,src=str(source),src_storage='local',dest=str(dest))}
    data={'pending':{'tv:show':dict(title='Show',first_seen=time.time()-8*86400)}}
    def save(key,value):
        """按真实 JSON 持久化边界复制数据。"""
        data[key]=json.loads(json.dumps(value))
    p.save_data=save
    p.get_data=lambda key:json.loads(json.dumps(data.get(key)))
    p._UnwatchedCleanup__get_plex=Mock(return_value=SimpleNamespace(query=Mock()))
    p._UnwatchedCleanup__collect=Mock(side_effect=lambda plex:[dict(item)])
    p._UnwatchedCleanup__history_since=Mock(return_value=set())
    p._UnwatchedCleanup__subscribed_tmdb_ids=Mock(return_value=set())
    p._UnwatchedCleanup__transfer_record=Mock(side_effect=records.get)
    p._UnwatchedCleanup__mp_api=Mock()
    p._UnwatchedCleanup__torrents_by_inode=Mock(return_value={})
    return p,item,source,dest,data

def test_success_removes_both_files_and_records_history(scenario):
    """源和目标完成删除后才清除待办并计入历史。"""
    p,item,source,dest,data=scenario
    def remove(*args):
        source.unlink();dest.unlink()
        return {'success':True}
    p._UnwatchedCleanup__mp_api.side_effect=remove
    p._UnwatchedCleanup__run()
    assert data['pending']=={}
    assert data['last_scan']['deleted']==1
    assert len(data['history'])==1

@pytest.mark.parametrize('failure',[False,True])
def test_failed_or_inaccurate_api_never_falls_back_to_library_delete(scenario,failure):
    """失败响应和不实成功响应都保留文件与重试计划。"""
    p,item,source,dest,data=scenario
    p._UnwatchedCleanup__mp_api.return_value={'success':failure,'message':'failure'}
    p._UnwatchedCleanup__run()
    assert source.exists() and dest.exists()
    assert data['last_scan']['deleted']==0
    assert data['pending']['tv:show']['cleanup_item']['records']


def test_partial_delete_retries_after_plex_item_disappears(scenario):
    """目标已删除但源删除失败时，下一轮仍能完成源清理。"""
    p,item,source,dest,data=scenario
    def partial(*args):
        dest.unlink()
        return {'success':False,'message':'source failed'}
    p._UnwatchedCleanup__mp_api.side_effect=partial
    p._UnwatchedCleanup__run()
    assert source.exists() and not dest.exists()
    p._UnwatchedCleanup__collect.side_effect=lambda plex:[]
    def finish(*args):
        source.unlink()
        return {'success':True}
    p._UnwatchedCleanup__mp_api.side_effect=finish
    p._UnwatchedCleanup__run()
    assert not source.exists() and not data['pending']
    assert data['last_scan']['deleted']==1

@pytest.mark.parametrize('protected',['played','subscribed','recent','grace','preview'])
def test_eligibility_guards(scenario,protected):
    """观看、订阅、新入库、宽限期和预览均不执行删除。"""
    p,item,source,dest,data=scenario
    if protected=='played':p._UnwatchedCleanup__history_since.return_value={'episode'}
    if protected=='subscribed':p._UnwatchedCleanup__subscribed_tmdb_ids.return_value={'123'}
    if protected=='recent':item['newest_added']=time.time()
    if protected=='grace':data['pending']['tv:show']['first_seen']=time.time()
    if protected=='preview':p._dry_run=True
    p._UnwatchedCleanup__run()
    p._UnwatchedCleanup__mp_api.assert_not_called()
    assert source.exists() and dest.exists()


def test_failed_downloader_preserves_library(scenario):
    """无整理记录且下载器清理失败时，媒体库文件保留。"""
    p,item,source,dest,data=scenario
    dest.unlink();os.link(source,dest)
    p._UnwatchedCleanup__transfer_record.side_effect=lambda f:None
    p._UnwatchedCleanup__torrents_by_inode.side_effect=RuntimeError('offline')
    p._UnwatchedCleanup__run()
    assert source.exists() and dest.exists()
    assert data['last_scan']['deleted']==0
    assert data['pending']['tv:show']['cleanup_item']['fallback_inodes']==[[dest.stat().st_dev,dest.stat().st_ino]]

@pytest.mark.parametrize('case',['match','different_device','brush_category','delete_failure'])
def test_torrent_matching_and_failures(scenario,monkeypatch,case):
    """仅匹配同设备硬链接，保留刷流分类并上报删除失败。"""
    p,item,source,dest,data=scenario
    qb=Mock()
    qb.is_inactive.return_value=False
    qb.get_torrents.return_value=([{'hash':'hash','name':'source','save_path':str(source.parent)+'/','category':'刷流' if case=='brush_category' else ''}],None)
    qb.get_files.return_value=[{'name':source.name}]
    qb.delete_torrents.return_value=case!='delete_failure'
    helper=Mock()
    helper.get_services.return_value={'qB':SimpleNamespace(instance=qb)}
    monkeypatch.setitem(namespace,'DownloaderHelper',lambda:helper)
    st=source.stat()
    identity=(st.st_dev+1 if case=='different_device' else st.st_dev,st.st_ino)
    invoke=lambda:Plugin._UnwatchedCleanup__torrents_by_inode(p,{identity})
    if case=='delete_failure':
        with pytest.raises(RuntimeError):invoke()
    elif case=='match':
        assert invoke()=={'source':['hash']}
    else:
        assert invoke()=={}
        qb.delete_torrents.assert_not_called()
