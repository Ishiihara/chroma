package dbmodel

type Notification struct {
	ID           int64  `gorm:"id;primaryKey;autoIncrement"`
	CollectionID string `gorm:"collection_id"`
	Type         string `gorm:"notification_type"`
	Status       string `gorm:"status"`
}

func (v Notification) TableName() string {
	return "notifications"
}

const (
	NotificationTypeCreateCollection = "create_collection"
	NotificationTypeDeleteCollection = "delete_collection"
)

const (
	NotificationStatusPending = "pending"
)

//go:generate mockery --name=INotificationDb
type INotificationDb interface {
	DeleteAll() error
	Delete(id []int64) error
	Insert(in *Notification) error
	GetAllPendingNotifications() ([]*Notification, error)
	GetNotificationByCollectionID(collectionID string) ([]*Notification, error)
}
