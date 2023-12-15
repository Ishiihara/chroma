-- Modify "collections" table
ALTER TABLE "public"."collections" ADD COLUMN "status" text NOT NULL;
-- Modify "segments" table
ALTER TABLE "public"."segments" ADD COLUMN "status" text NOT NULL;
