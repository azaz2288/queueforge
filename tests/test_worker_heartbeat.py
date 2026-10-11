"""Synthetic long handlers, real process loss and heartbeat fencing."""
import math
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from queueforge.queue import Queue, work_once


class WorkerHeartbeatTests(unittest.TestCase):
    def test_long_handler_renewed_and_competing_claim_cannot_take_live_job(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite');job,_=q.submit('long','text_stats','synthetic text')
            entered=threading.Event();release=threading.Event();errors=[]
            def long(job):entered.set();release.wait(5);return {'synthetic':True}
            def worker():
                try:work_once(q,'owner',lease_seconds=.2,heartbeat_interval=.04)
                except BaseException as error:errors.append(error)
            with patch('queueforge.queue.execute',side_effect=long):
                thread=threading.Thread(target=worker);thread.start()
                try:
                    self.assertTrue(entered.wait(3));time.sleep(.45)
                    self.assertIsNone(Queue(q.path).claim('competitor',.2))
                    self.assertGreaterEqual(sum(e['kind']=='heartbeat' for e in q.history(job['id'])),2)
                finally:release.set();thread.join(5)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            self.assertEqual(q.metrics(),{'succeeded':1});self.assertEqual(q.list()[0]['attempts'],1)

    def test_invalid_heartbeat_configuration_refused_before_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite');q.submit('valid','sha256','fixture')
            for lease,interval in ((True,.1),(.09,.01),(301,.1),(math.nan,.1),(.3,True),(.3,0),(.3,.3),(.3,math.inf)):
                with self.subTest(lease=lease,interval=interval),self.assertRaises(ValueError):
                    work_once(q,'invalid',lease_seconds=lease,heartbeat_interval=interval)
                self.assertEqual(q.metrics(),{'pending':1});self.assertEqual(q.list()[0]['attempts'],0)

    def test_heartbeat_failure_refuses_result_and_preserves_recoverable_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite');job,_=q.submit('error','sha256','fixture')
            with (patch.object(q,'heartbeat',side_effect=OSError('synthetic storage failure')) as heartbeat,
                 patch('queueforge.queue.execute',side_effect=lambda _:time.sleep(.15) or {'ok':True})):
                self.assertTrue(work_once(q,'owner',lease_seconds=.5,heartbeat_interval=.03))
            self.assertTrue(heartbeat.called)
            state=q.list()[0];self.assertEqual(state['status'],'running');self.assertIsNone(state['result'])
            self.assertFalse(any(e['kind'] in ('succeeded','failed') for e in q.history(job['id'])))
            time.sleep(.5);self.assertEqual(q.claim('recovery',.5)['id'],job['id'])

    def test_cancelled_long_handler_never_publishes_or_requeues_cancelled_job(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite');job,_=q.submit('cancel','sha256','fixture')
            def cancel(_):q.cancel(job['id']);time.sleep(.08);return {'stale':True}
            with patch('queueforge.queue.execute',side_effect=cancel):
                self.assertTrue(work_once(q,'owner',lease_seconds=.5,heartbeat_interval=.02))
            self.assertEqual(q.metrics(),{'cancelled':1});self.assertIsNone(q.list()[0]['result'])

    def test_handler_failure_and_keyboard_interrupt_stop_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite');q.submit('failure','sha256','fixture')
            with patch('queueforge.queue.execute',side_effect=ValueError('synthetic failure')):
                self.assertTrue(work_once(q,'owner',lease_seconds=.3,heartbeat_interval=.03))
            self.assertEqual(q.metrics(),{'pending':1});self.assertEqual(q.list()[0]['attempts'],1)
            q.submit('interrupt','sha256','fixture')
            with patch('queueforge.queue.execute',side_effect=KeyboardInterrupt),self.assertRaises(KeyboardInterrupt):
                work_once(q,'owner',lease_seconds=.3,heartbeat_interval=.03)
            before=len(q.history(q.list()[0]['id']));time.sleep(.1)
            self.assertEqual(len(q.history(q.list()[0]['id'])),before)
            self.assertFalse(any(t.name.startswith('queueforge-heartbeat-') for t in threading.enumerate()))

    def test_real_heartbeat_worker_kill_recovered_after_last_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'queue.sqlite';q=Queue(path);job,_=q.submit('kill','sha256','fixture')
            marker=Path(directory)/'entered'
            code='''
import pathlib,sys,time
from queueforge.queue import Queue,work_once
import queueforge.queue as module
def slow(job):
    pathlib.Path(sys.argv[2]).touch()
    time.sleep(60)
    return {'never':'published'}
module.execute=slow
work_once(Queue(sys.argv[1]),'killed',lease_seconds=.5,heartbeat_interval=.05)
'''
            child=subprocess.Popen([sys.executable,'-c',code,str(path),str(marker)],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            try:
                until=time.monotonic()+5
                while time.monotonic()<until and child.poll() is None:
                    if marker.exists() and any(e['kind']=='heartbeat' for e in q.history(job['id'])):break
                    time.sleep(.02)
                self.assertTrue(marker.exists());self.assertTrue(any(e['kind']=='heartbeat' for e in q.history(job['id'])))
                child.kill();child.communicate(timeout=5);old=q.list()[0]
                time.sleep(max(0,old['expires']-time.time())+.12)
                recovered=q.claim('restarted',1);self.assertEqual(recovered['id'],job['id'])
                self.assertEqual(recovered['attempts'],2);self.assertNotEqual(recovered['token'],old['token'])
                from queueforge.queue import Conflict
                with self.assertRaises(Conflict):q.finish(job['id'],old['token'],{'stale':True})
                q.finish(job['id'],recovered['token'],{'recovered':True})
                self.assertEqual(q.metrics(),{'succeeded':1})
            finally:
                if child.poll() is None:child.kill()
                child.communicate(timeout=5)

    def test_fast_success_and_empty_queue_leave_no_renewal_threads(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite')
            self.assertFalse(work_once(q,'idle',lease_seconds=.3))
            q.submit('fast','sha256','fixture')
            self.assertTrue(work_once(q,'fast',lease_seconds=.3))
            self.assertEqual(q.metrics(),{'succeeded':1})
            self.assertFalse(any(t.name.startswith('queueforge-heartbeat-') for t in threading.enumerate()))

    def test_thread_start_failure_does_not_execute_and_lease_can_expire(self):
        with tempfile.TemporaryDirectory() as directory:
            now=[100.0];q=Queue(Path(directory)/'queue.sqlite',clock=lambda:now[0])
            job,_=q.submit('startup','sha256','fixture')
            with (patch('queueforge.queue.threading.Thread.start',side_effect=RuntimeError('synthetic startup failure')),
                  patch('queueforge.queue.execute') as execute,
                  self.assertRaises(RuntimeError)):
                work_once(q,'failed-start',lease_seconds=1)
            execute.assert_not_called();self.assertEqual(q.metrics(),{'running':1})
            now[0]=101.0;self.assertEqual(q.claim('recovery',1)['id'],job['id'])
            self.assertEqual(q.list()[0]['attempts'],2)

    def test_boolean_duration_rejected_by_direct_claim_and_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            q=Queue(Path(directory)/'queue.sqlite');q.submit('bool','sha256','fixture')
            with self.assertRaises(ValueError):q.claim('bool',True)
            job=q.claim('owner',1)
            before=q.list()[0]['expires']
            with self.assertRaises(ValueError):q.heartbeat(job['id'],job['token'],True)
            self.assertEqual(q.list()[0]['expires'],before)

if __name__=='__main__':unittest.main()
