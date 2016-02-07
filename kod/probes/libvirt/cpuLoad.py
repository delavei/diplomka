import libvirt
import sys
import time

conn = libvirt.openReadOnly(None)
if conn == None:
    print 'Failed to open connection to the hypervisor'
    sys.exit(1)

try:
    dom0 = conn.lookupByName("debian8")
except:
    print 'Failed to find the main domain'
    sys.exit(1)

print "Domain 0: id %d running %s" % (dom0.ID(), dom0.OSType())

while True:
	domCpu = dom0.getCPUStats(-1, 0).pop(0)['cpu_time']

	hostCpu = conn.getCPUStats(-1, 0)['kernel']
	bla = domCpu/ float (hostCpu)
	print bla

	time.sleep(1)
