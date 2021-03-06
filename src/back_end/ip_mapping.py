import socket
import paramiko as pk
from paramiko import AuthenticationException
import pickle
import os

slave_ids = ['10.60.45.11', '10.60.45.10', '10.60.45.19', '10.60.45.22']
remote_usrs = ['liuyajun', 'liuyajun', 'lyj', 'jishi511']
remote_dirs = ['/home/liuyajun/django_server/DistributedSystemFinal/data_files',
               '/home/liuyajun/django_server/DistributedSystemFinal/data_files/',
               '/home/lyj/django_server/DistributedSystemFinal/data_files/',
               '/large_disk/lyj/django_server/DistributedSystemFinal/data_files/']


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]
    finally:
        s.close()


def send_chunk_to_slave(slave_ip, local_path, remote_path, usr):
    psd_f = open(os.path.join(os.environ['HOME'], 'django_server/psd.pkl'), 'r')
    psd_dict = pickle.load(psd_f)
    psd_f.close()
    try:
        ssh = pk.SSHClient()
        ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
        ssh.connect(hostname=slave_ip, port=22, username=usr, password=psd_dict[slave_ip])

        sftp = pk.SFTPClient.from_transport(ssh.get_transport())
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()
        return True
    except AuthenticationException:
        print('Exception when visiting %s' % slave_ip)
        return False
    except IOError:
        print('IO Exception when visiting %s' % slave_ip)
        return False


def get_chunk_from_slave(slave_ip, local_path, remote_path, usr):
    psd_f = open(os.path.join(os.environ['HOME'], 'django_server/psd.pkl'), 'r')
    psd_dict = pickle.load(psd_f)
    psd_f.close()
    try:
        ssh = pk.SSHClient()
        ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
        ssh.connect(hostname=slave_ip, port=22, username=usr, password=psd_dict[slave_ip])

        sftp = pk.SFTPClient.from_transport(ssh.get_transport())
        sftp.get(remote_path, local_path)
        sftp.close()
        ssh.close()
    except AuthenticationException:
        return False
    except IOError:
        print('IO Exception when visiting %s' % slave_ip)
    return True


def map_chunk_to_slave(chunk_path, chunk_idx, chunk_number):
    slice_num = chunk_number / 5
    f_name = chunk_path.split('/')[-1]
    local_ip = get_ip()

    if chunk_idx < slice_num * 3 and not local_ip == slave_ids[0]:
        flag = send_chunk_to_slave(slave_ids[0], chunk_path,
                                   os.path.join(remote_dirs[0], f_name),
                                   remote_usrs[0])
    if slice_num <= chunk_idx < slice_num * 2 or \
       slice_num * 3 <= chunk_idx < slice_num * 4:
        if not local_ip == slave_ids[1]:
            flag = send_chunk_to_slave(slave_ids[1], chunk_path,
                                       os.path.join(remote_dirs[1], f_name),
                                       remote_usrs[1])
    if slice_num * 2 <= chunk_idx < slice_num * 3 or \
       slice_num * 4 <= chunk_idx < chunk_number:
        if not local_ip == slave_ids[2]:
            flag = send_chunk_to_slave(slave_ids[2], chunk_path,
                                       os.path.join(remote_dirs[2], f_name),
                                       remote_usrs[2])
    if chunk_idx < slice_num or slice_num * 3 <= chunk_idx < chunk_number:
        if not local_ip == slave_ids[3]:
            flag = send_chunk_to_slave(slave_ids[3], chunk_path,
                                       os.path.join(remote_dirs[3], f_name),
                                       remote_usrs[3])

    slave_chunk_range = [[(0, 3 * slice_num)],
                         [(slice_num, 2 * slice_num), (slice_num * 3, slice_num * 4)],
                         [(slice_num * 2, slice_num * 3), (slice_num * 4, chunk_number)],
                         [(0, slice_num), (slice_num * 3, chunk_number)]]

    local_range = slave_chunk_range[slave_ids.index(local_ip)]
    local_flag = False
    for rng in local_range:
        if rng[0] <= chunk_idx < rng[1]:
            local_flag = True
    if not local_flag:
        os.remove(chunk_path)
