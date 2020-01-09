package k8sutil

import (
	"fmt"
	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	namespace = "magellan"
)

type MagellanK8sUtil struct {
	log *logrus.Entry

	KubeCli   kubernetes.Interface

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

	volumeAttachment, err := mk.GetVolumeAttachments(pvName, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	nodeName := volumeAttachment.Spec.NodeName
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{nodeName},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (mk *MagellanK8sUtil) GetBoundedPvNameByPvcName(pvcName string, namespace string) (string, error) {
	pvc, err := mk.GetPvc(pvcName, namespace)
	if err != nil {
		return "", err
	}

	if pvc.Status.Phase == corev1.ClaimBound {
		mk.log.Infof("Found bounded pv=%s for pvc=%s", pvc.Spec.VolumeName, pvcName)
		return pvc.Spec.VolumeName, nil
	} else {
		return "", fmt.Errorf("Fail to find bounded pv to pvc name %s!", pvcName)
	}
}

func (mk *MagellanK8sUtil) GetPvc(name string, namespace string) (*corev1.PersistentVolumeClaim, error) {
	pvcs, err := mk.KubeCli.CoreV1().PersistentVolumeClaims(namespace).List(metav1.ListOptions{})
	if err != nil {
		mk.log.Infof("Error listing pvs: %v", err)
		return nil, err
	}

	for _, pvc := range pvcs.Items {
		if pvc.Name == name && pvc.Namespace == namespace {
			return &pvc, nil
		}
	}
	return nil, fmt.Errorf("%s",name)
}

func (mk *MagellanK8sUtil) GetPv(name string) (pv *corev1.PersistentVolume, err error) {
	mk.log.Infof("Get PV name %s ", name)
	pv, err = mk.KubeCli.CoreV1().PersistentVolumes().Get(name, metav1.GetOptions{})
	if err != nil {
		mk.log.Errorf("Got exception on PV scan %s ", pv)
		return nil, fmt.Errorf("Got exception on PV scan %s ", err)
	}
	return pv, nil
}

func (mk *MagellanK8sUtil) GetVolumeAttachments(pvName string, listOptions metav1.ListOptions) (*storagev1beta1.VolumeAttachment, error) {
	volumeAttachments, err := mk.KubeCli.StorageV1beta1().VolumeAttachments().List(listOptions)
	if err != nil {
		return nil, err
	}

	for _, volAtt := range volumeAttachments.Items {
		if *volAtt.Spec.Source.PersistentVolumeName == pvName {
			mk.log.Infof("volume attachments %s compatible to pv %s", volAtt.Name, pvName)
			return &volAtt, nil
		}
	}
	return nil, fmt.Errorf("Got exception on volataach scan %s ", err)
}
