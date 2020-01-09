package k8sutil

import (
	"fmt"
	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strconv"
	"strings"
	"time"
)

const (
	namespace = "magellan"

	MagellanAppName                = "app"
	DiskProxyAppLabel              = MagellanAppName + "=disk-proxy"
	DiskProxyMediaBdfAnnotationKey = "disks"

	MaxRetries          = 35
	SleepBetweenRetries = 5 * time.Second
)

type MagellanK8sUtil struct {
	log *logrus.Entry

	KubeCli kubernetes.Interface
}

func New(KubeCli kubernetes.Interface, cl *api.EtcdCluster) *MagellanK8sUtil {
	lg := logrus.WithField("pkg", "cluster").WithField("cluster-name", cl.Name).WithField("cluster-namespace", cl.Namespace)
	return &MagellanK8sUtil{KubeCli: KubeCli, log: lg}
}

func (mk *MagellanK8sUtil) CreateNodeAffinity(pvcName string) (*corev1.Affinity, error) {
	pvName, err := mk.GetBoundedPvNameByPvcName(pvcName, namespace)
	if err != nil {
		return nil, err
	}

	volumeId, err := mk.GetVolumeIdByPvName(pvName)
	if err != nil {
		return nil, err
	}

	deviceId, err := ConvertStringToUint64(volumeId)
	if err != nil {
		return nil, err
	}

	diskProxySts, err := mk.GetDiskProxyStatefulByDeviceId(deviceId)
	if err != nil {
		return nil, err
	}

	return diskProxySts.Spec.Template.Spec.Affinity, nil
}

func (mk *MagellanK8sUtil) GetBoundedPvNameByPvcName(pvcName string, namespace string) (string, error) {
	for i := 0; i < MaxRetries; i++ {
		pvc, err := mk.GetPvc(pvcName, namespace)
		if err != nil {
			return "", err
		}

		if pvc.Status.Phase == corev1.ClaimBound {
			mk.log.Infof("Found bounded pv=%s for pvc=%s", pvc.Spec.VolumeName, pvcName)
			return pvc.Spec.VolumeName, nil
		}

		mk.log.Warnf("Waiting %d seconds before retry getting pvc  %s", SleepBetweenRetries/time.Second, pvcName)
		time.Sleep(SleepBetweenRetries)
	}
	return "", fmt.Errorf("fail to find bounded pv to pvc name %s", pvcName)
}

func (mk *MagellanK8sUtil) GetPvc(name string, namespace string) (persistentVolumeClaim *corev1.PersistentVolumeClaim, err error) {
	pvcs, err := mk.KubeCli.CoreV1().PersistentVolumeClaims(namespace).List(metav1.ListOptions{})
	if err != nil {
		mk.log.Errorf("Error listing pvcs: %v", err)
		return nil, err
	}

	for _, pvc := range pvcs.Items {
		if pvc.Name == name && pvc.Namespace == namespace {
			mk.log.Infof("found pvc: %s", name)
			return &pvc, nil
		}
	}
	return nil, fmt.Errorf("fail to find pvc by name %s", name)
}

func (mk *MagellanK8sUtil) GetVolumeIdByPvName(pvName string) (volId string, err error) {
	mk.log.Infof("Start getVolumeByPvName, searching for name=%s", pvName)

	pv, err := mk.GetPvByName(pvName)
	if err != nil {
		return "", fmt.Errorf("Error while trying to get volume by pv name %s. Error: %v ", pvName, err)
	}
	if pv.Spec.CSI == nil {
		return "", fmt.Errorf("Error while trying to get volume by pv name %s. Error: pv.Spec.CSI=nil ", pvName)
	}
	volId = strings.TrimSpace(pv.Spec.CSI.VolumeHandle)
	if len(volId) == 0 {
		return "", fmt.Errorf("Error while trying to get volume by pv name %s. Error: pv.Spec.CSI.VolumeHandle=%s ", pvName, pv.Spec.CSI.VolumeHandle)
	}
	return volId, nil
}

func (mk *MagellanK8sUtil) GetPvByName(name string) (pv *corev1.PersistentVolume, err error) {
	mk.log.Infof("Get PV name %s ", name)
	pv, err = mk.KubeCli.CoreV1().PersistentVolumes().Get(name, metav1.GetOptions{})
	if err != nil {
		mk.log.Errorf("Got exception on PV scan %s ", pv)
		return nil, fmt.Errorf("Got exception on PV scan %s ", err)
	}
	return pv, nil
}

func (mk *MagellanK8sUtil) GetDiskProxyStatefulByDeviceId(deviceId uint64) (diskProxy *appsv1.StatefulSet, err error) {
	diskProxyList, err := mk.KubeCli.AppsV1().StatefulSets(namespace).List(metav1.ListOptions{LabelSelector: DiskProxyAppLabel})
	if err != nil {
		mk.log.Errorf("Failed to find disk proxy statefulSet list with by label %s", DiskProxyAppLabel)
		return nil, err
	}
	for _, dp := range diskProxyList.Items {
		devicesList := dp.Spec.Template.Annotations[DiskProxyMediaBdfAnnotationKey]
		mk.log.Debugf("searching matching %s for device id %d in disks list: %s", dp.Name, deviceId, devicesList)
		for _, deviceTuple := range strings.Split(devicesList, ",") {
			deviceIdStr := strings.Split(deviceTuple, " ")[0]
			if deviceIdStr == fmt.Sprint(deviceId) {
				return &dp, nil
			}
		}
	}
	return nil, fmt.Errorf("unable to find matching disk proxy by device id %d", deviceId)
}

func ConvertStringToUint64(str string) (uint64, error) {
	return strconv.ParseUint(str, 10, 64)
}
