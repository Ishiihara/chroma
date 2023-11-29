package notification

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dao"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSimpleNotificationProcessor(t *testing.T) {
	ctx := context.Background()
	db := setupDatabase()
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	notificationStore := NewDatabaseNotificationStore(txnImpl, metaDomain)
	notifier := NewMemoryNotifier()
	notificationProcessor := NewSimpleNotificationProcessor(ctx, notificationStore, notifier)
	notificationProcessor.Start()

	notification := model.Notification{
		CollectionID: "collection1",
		Type:         model.NotificationTypeDeleteCollection,
		Status:       model.NotificationStatusPending,
	}
	resultChan := make(chan error)
	triggerMsg := TriggerMessage{
		Msg:        notification,
		ResultChan: resultChan,
	}
	notificationStore.AddNotification(ctx, notification)
	notificationProcessor.Trigger(ctx, triggerMsg)

	for _, msg := range notifier.queue {
		newMsg := model.Notification{}
		err := json.Unmarshal(msg.Payload, &newMsg)
		if err != nil {
			t.Errorf("Failed to unmarshal message %v", err)
		}
		if newMsg.CollectionID != notification.CollectionID {
			t.Errorf("CollectionID is not equal %v, %v", newMsg.CollectionID, notification.CollectionID)
		}
		if newMsg.Type != notification.Type {
			t.Errorf("Type is not equal %v, %v", newMsg.Type, notification.Type)
		}
		if newMsg.Status != notification.Status {
			t.Errorf("Status is not equal, %v, %v", newMsg.Status, notification.Status)
		}
	}
	notificationProcessor.Stop()
	cleanupDatabase(db)
}

func TestSimpleNotificationProcessorWithExistingNotification(t *testing.T) {
	ctx := context.Background()
	db := setupDatabase()
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	notificationStore := NewDatabaseNotificationStore(txnImpl, metaDomain)
	notifier := NewMemoryNotifier()
	notificationProcessor := NewSimpleNotificationProcessor(ctx, notificationStore, notifier)

	notification := model.Notification{
		CollectionID: "collection1",
		Type:         model.NotificationTypeDeleteCollection,
		Status:       model.NotificationStatusPending,
	}
	// Only add to the notification store, but not trigger it.
	notificationStore.AddNotification(ctx, notification)

	notificationProcessor.Start()

	for _, msg := range notifier.queue {
		newMsg := model.Notification{}
		err := json.Unmarshal(msg.Payload, &newMsg)
		if err != nil {
			t.Errorf("Failed to unmarshal message %v", err)
		}
		if newMsg.CollectionID != notification.CollectionID {
			t.Errorf("CollectionID is not equal %v, %v", newMsg.CollectionID, notification.CollectionID)
		}
		if newMsg.Type != notification.Type {
			t.Errorf("Type is not equal %v, %v", newMsg.Type, notification.Type)
		}
		if newMsg.Status != notification.Status {
			t.Errorf("Status is not equal, %v, %v", newMsg.Status, notification.Status)
		}
	}
	notificationProcessor.Stop()
	cleanupDatabase(db)
}

func TestSimpleNotificationProcessorCleanShutdown(t *testing.T) {
	ctx := context.Background()
	db := setupDatabase()
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	notificationStore := NewDatabaseNotificationStore(txnImpl, metaDomain)
	notifier := NewMemoryNotifier()
	notificationProcessor := NewSimpleNotificationProcessor(ctx, notificationStore, notifier)

	notification := model.Notification{
		CollectionID: "collection1",
		Type:         model.NotificationTypeDeleteCollection,
		Status:       model.NotificationStatusPending,
	}

	notificationProcessor.Start()

	// Only add to the notification store, but not trigger it.
	notificationStore.AddNotification(ctx, notification)
	notificationProcessor.Stop()

	for _, msg := range notifier.queue {
		newMsg := model.Notification{}
		err := json.Unmarshal(msg.Payload, &newMsg)
		if err != nil {
			t.Errorf("Failed to unmarshal message %v", err)
		}
		if newMsg.CollectionID != notification.CollectionID {
			t.Errorf("CollectionID is not equal %v, %v", newMsg.CollectionID, notification.CollectionID)
		}
		if newMsg.Type != notification.Type {
			t.Errorf("Type is not equal %v, %v", newMsg.Type, notification.Type)
		}
		if newMsg.Status != notification.Status {
			t.Errorf("Status is not equal, %v, %v", newMsg.Status, notification.Status)
		}
	}
	cleanupDatabase(db)
}

func setupDatabase() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic("failed to connect database")
	}
	dbcore.SetGlobalDB(db)
	db.Migrator().CreateTable(&dbmodel.Notification{})
	return db
}

func cleanupDatabase(db *gorm.DB) {
	db.Migrator().DropTable(&dbmodel.Notification{})
	dbcore.SetGlobalDB(nil)
}
