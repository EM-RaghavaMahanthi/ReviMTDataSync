DROP TABLE IF EXISTS "mt_membership_transactions_details_dlk";
DROP TABLE IF EXISTS "mt_credit_transactions_details_dlk";
DROP TABLE IF EXISTS "mt_order_lines_details_dlk";
DROP TABLE IF EXISTS "mt_customers_details_dlk";
DROP TABLE IF EXISTS "mt_class_sessions_details_dlk";
DROP TABLE IF EXISTS "mt_membership_instances_details_dlk";
DROP TABLE IF EXISTS "mt_reservations_details_dlk";
DROP TABLE IF EXISTS "mt_orders_details_dlk";

CREATE TABLE "mt_membership_transactions_details_dlk" (
  id integer,
  membership_transactions_id integer,
  transaction_date timestamp(3) without time zone,
  membership_name character varying(128),
  parent_membership_transaction_id integer,
  membership_instances_id integer,
  customer_id character varying(255),
  location integer,
  payment_interval_end_date timestamp(3) without time zone,
  isin_order_line boolean,
  isin_reservation boolean,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  account_id integer,
  customer_ref_id integer,
  membership_instances_ref_id integer
);

CREATE TABLE "mt_customers_details_dlk" (
  id integer,
  customer_id character varying(255),
  location_id integer,
  account_id integer,
  first_name character varying(64),
  last_name character varying(64),
  email character varying(255),
  full_name character varying(255),
  birth_date timestamp(3) without time zone,
  birth_month integer,
  birth_day integer,
  phone_number character varying(64),
  address_line1 text,
  address_line2 text,
  address_line3 text,
  city character varying(255),
  country character varying(255),
  state_province character varying(255),
  customer_state character varying(64),
  postal_code character varying(64),
  gender character varying(64),
  date_joined timestamp(3) without time zone,
  is_opted_in_to_sms boolean,
  completed_class_count integer,
  state_id integer,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer
);


CREATE TABLE "mt_reservations_details_dlk" (
  id integer,
  reservations_id integer,
  cancel_date timestamp(3) without time zone,
  check_in_date timestamp(3) without time zone,
  creation_date timestamp(3) without time zone,
  status character varying(16),
  credit_transactions_type character varying(64),
  credit_transactions_id integer,
  membership_transactions_type character varying(64),
  membership_transactions_id integer,
  guest boolean,
  customer_id character varying(255),
  location integer,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  first_timer boolean,
  class_session_id character varying(255),
  reservation_type character varying(64),
  account_id integer,
  class_session_ref_id integer,
  credit_transactions_ref_id integer,
  customer_ref_id integer,
  membership_transactions_ref_id integer,
  transaction_type character varying(64)
);


CREATE TABLE "mt_class_sessions_details_dlk" (
  id integer,
  class_session_id text,
  start_datetime timestamp(3) without time zone,
  start_date text,
  location text,
  end_datetime timestamp(3) without time zone,
  cancellation_datetime timestamp(3) without time zone,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  account_id integer
);


CREATE TABLE "mt_order_lines_details_dlk" (
  id integer,
  order_line_id character varying(64),
  order_id text,
  transaction_type character varying(255),
  location character varying(255),
  credit_transactions_id integer,
  membership_transactions_id integer,
  title character varying(255),
  processed_by boolean,
  child_orders text,
  is_valid boolean DEFAULT TRUE,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  account_id integer,
  credit_transactions_ref_id integer,
  membership_transactions_ref_id integer,
  order_ref_id integer
);


CREATE TABLE "mt_orders_details_dlk" (
  id integer,
  order_id text,
  date_placed timestamp(3) without time zone,
  location text,
  location_id integer,
  payment_sources_labels character varying(255),
  status character varying(255),
  order_lines_id character varying(64),
  customer_id character varying(255),
  parent_order character varying(255),
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  account_id integer,
  customer_ref_id integer
);

CREATE TABLE "mt_credit_transactions_details_dlk" (
  id integer,
  credit_transactions_id integer,
  transaction_date timestamp(3) without time zone,
  credit_name character varying(128),
  is_expired boolean,
  remaining_credits_cache integer,
  is_intro_offer boolean,
  parent_credit_transaction_type character varying(64),
  parent_credit_transaction_id integer,
  customer_id character varying(255),
  location integer,
  isin_order_line boolean,
  isin_reservation boolean,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  account_id integer,
  customer_ref_id integer
);

CREATE TABLE "mt_membership_instances_details_dlk" (
  id integer,
  membership_instances_id integer,
  purchase_date timestamp(3) without time zone,
  membership_name character varying(128),
  renewal_rate_incl_tax character varying(16),
  status character varying(128),
  location integer,
  renewal_count integer,
  next_charge_date timestamp(3) without time zone,
  created_at timestamp(3) without time zone,
  created_by integer,
  updated_at timestamp(3) without time zone,
  updated_by integer,
  deleted_at timestamp(3) without time zone,
  deleted_by integer,
  account_id integer
);