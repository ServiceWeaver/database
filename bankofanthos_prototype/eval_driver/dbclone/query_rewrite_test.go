package dbclone

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestQueryRewrite(t *testing.T) {
	ctx := context.Background()

	// Setup database
	dbContainer, connPool, _, err := SetupTestDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dbContainer.Terminate(ctx)

	err = createTables(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	database, err := newDatabase(ctx, connPool)
	if err != nil {
		t.Fatal(err)
	}

	cloneDdl, err := newCloneDdl(ctx, database, "test")
	if err != nil {
		t.Fatal(err)
	}

	defer cloneDdl.close(ctx)
	defer cloneDdl.reset(ctx)

	t.Run("InsertTriggersForUsers", func(t *testing.T) {
		err = createInsertTriggers(ctx, connPool, cloneDdl.clonedTables["users"])
		if err != nil {
			t.Fatal(err)
		}

		triggers, err := database.getTableTriggers(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := trigger{
			Name:              "users_redirect_insert_trigger",
			EventManipulation: "INSERT",
			ActionStatement:   "EXECUTE FUNCTION users_redirect_insert()",
			ActionOrientation: "ROW",
			ActionTiming:      "INSTEAD OF",
			Procedure: &procedure{
				Name: "users_redirect_insert",
				ProSrc: `
				BEGIN
				IF EXISTS (SELECT * FROM users WHERE (accountid) = (NEW.accountid)) THEN
					RAISE EXCEPTION 'column % already exists', NEW.accountid;
				END IF;
				IF EXISTS (SELECT * FROM users WHERE (username) = (NEW.username)) THEN
					RAISE EXCEPTION 'column % already exists', NEW.username;
				END IF;
				INSERT INTO test.usersplus (accountid, birthday, passhash, username)    
				VALUES (NEW.accountid, NEW.birthday, NEW.passhash, NEW.username);
				RETURN NEW;
				END;
				`,
			},
		}

		if diff := cmp.Diff(expectedTriggers, triggers["users_redirect_insert_trigger"], procOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("InsertTriggersForContacts", func(t *testing.T) {
		err = createInsertTriggers(ctx, connPool, cloneDdl.clonedTables["contacts"])
		if err != nil {
			t.Fatal(err)
		}

		triggers, err := database.getTableTriggers(ctx, "contacts")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := trigger{
			Name:              "contacts_redirect_insert_trigger",
			EventManipulation: "INSERT",
			ActionStatement:   "EXECUTE FUNCTION contacts_redirect_insert()",
			ActionOrientation: "ROW",
			ActionTiming:      "INSTEAD OF",
			Procedure: &procedure{
				Name: "contacts_redirect_insert",
				ProSrc: `
				BEGIN
				IF NOT EXISTS (SELECT * FROM users WHERE (username) = (NEW.username)) THEN
				RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in  users table';
				END IF;
				INSERT INTO test.contactsplus (account_num, is_external, username)    
				VALUES (NEW.account_num, NEW.is_external, NEW.username);
				RETURN NEW;
				END;
				`,
			},
		}

		if diff := cmp.Diff(expectedTriggers, triggers["contacts_redirect_insert_trigger"], procOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("UpdateTriggersForUsers", func(t *testing.T) {
		err = createUpdateTriggers(ctx, connPool, cloneDdl.clonedTables["users"])
		if err != nil {
			t.Fatal(err)
		}

		triggers, err := database.getTableTriggers(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := trigger{
			Name:              "users_redirect_update_trigger",
			EventManipulation: "UPDATE",
			ActionStatement:   "EXECUTE FUNCTION users_redirect_update()",
			ActionOrientation: "ROW",
			ActionTiming:      "INSTEAD OF",
			Procedure: &procedure{
				Name: "users_redirect_update",
				ProSrc: `
				BEGIN
				IF EXISTS (SELECT * FROM users WHERE (accountid) = (NEW.accountid)) AND (NEW.accountid) != (OLD.accountid) THEN
					RAISE EXCEPTION 'column % already exists', NEW.accountid;
				END IF;
				IF EXISTS (SELECT * FROM users WHERE (username) = (NEW.username)) AND (NEW.username) != (OLD.username) THEN
                	RAISE EXCEPTION 'column % already exists', NEW.username;
       			END IF;
				IF EXISTS (SELECT * FROM contacts WHERE (username) = (OLD.username)) AND (NEW.username) != (OLD.username) THEN
				RAISE EXCEPTION 'violates foreign key constraint';
				END IF;
				INSERT INTO test.usersminus (accountid, birthday, passhash, username) VALUES (OLD.accountid, OLD.birthday, OLD.passhash, OLD.username);
				INSERT INTO test.usersplus (accountid, birthday, passhash, username) VALUES (NEW.accountid, NEW.birthday, NEW.passhash, NEW.username);
				RETURN NEW;
				END;
				`,
			},
		}

		if diff := cmp.Diff(expectedTriggers, triggers["users_redirect_update_trigger"], procOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("UpdateTriggersForContacts", func(t *testing.T) {
		err = createUpdateTriggers(ctx, connPool, cloneDdl.clonedTables["contacts"])
		if err != nil {
			t.Error(err)
		}

		triggers, err := database.getTableTriggers(ctx, "contacts")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := trigger{
			Name:              "contacts_redirect_update_trigger",
			EventManipulation: "UPDATE",
			ActionStatement:   "EXECUTE FUNCTION contacts_redirect_update()",
			ActionOrientation: "ROW",
			ActionTiming:      "INSTEAD OF",
			Procedure: &procedure{
				Name: "contacts_redirect_update",
				ProSrc: `
				BEGIN
				IF NOT EXISTS (SELECT * FROM users WHERE (username) = (NEW.username)) THEN
				RAISE EXCEPTION 'violates foreign key constraint, forigen key does not exist in users table';
				END IF;
				INSERT INTO test.contactsminus (account_num, is_external, username) VALUES (OLD.account_num, OLD.is_external, OLD.username);
				INSERT INTO test.contactsplus (account_num, is_external, username) VALUES (NEW.account_num, NEW.is_external, NEW.username);
				RETURN NEW;
				END;
				`,
			},
		}

		if diff := cmp.Diff(expectedTriggers, triggers["contacts_redirect_update_trigger"], procOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("DeleteTriggersForUsers", func(t *testing.T) {
		err = createDeleteTriggers(ctx, connPool, cloneDdl.clonedTables["users"])
		if err != nil {
			t.Fatal(err)
		}

		triggers, err := database.getTableTriggers(ctx, "users")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := trigger{
			Name:              "users_redirect_delete_trigger",
			EventManipulation: "DELETE",
			ActionStatement:   "EXECUTE FUNCTION users_redirect_delete()",
			ActionOrientation: "ROW",
			ActionTiming:      "INSTEAD OF",
			Procedure: &procedure{
				Name: "users_redirect_delete",
				ProSrc: `
				BEGIN
				IF EXISTS (SELECT * FROM contacts WHERE (username) = (OLD.username)) THEN
				RAISE EXCEPTION 'violates foreign key constraint';
				END IF;
				INSERT INTO test.usersminus (accountid, birthday, passhash, username) VALUES (OLD.accountid, OLD.birthday, OLD.passhash, OLD.username);
				RETURN OLD;
				END;
				`,
			},
		}

		if diff := cmp.Diff(expectedTriggers, triggers["users_redirect_delete_trigger"], procOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})

	t.Run("DeleteTriggersForContacts", func(t *testing.T) {
		err = createDeleteTriggers(ctx, connPool, cloneDdl.clonedTables["contacts"])
		if err != nil {
			t.Fatal(err)
		}

		triggers, err := database.getTableTriggers(ctx, "contacts")
		if err != nil {
			t.Fatal(err)
		}
		expectedTriggers := trigger{
			Name:              "contacts_redirect_delete_trigger",
			EventManipulation: "DELETE",
			ActionStatement:   "EXECUTE FUNCTION contacts_redirect_delete()",
			ActionOrientation: "ROW",
			ActionTiming:      "INSTEAD OF",
			Procedure: &procedure{
				Name: "contacts_redirect_delete",
				ProSrc: `
				BEGIN
				INSERT INTO test.contactsminus (account_num, is_external, username) VALUES (OLD.account_num, OLD.is_external, OLD.username);
				RETURN OLD;
				END;
				`,
			},
		}

		if diff := cmp.Diff(expectedTriggers, triggers["contacts_redirect_delete_trigger"], procOpt); diff != "" {
			t.Errorf("(-want,+got):\n%s", diff)
		}
	})
}
