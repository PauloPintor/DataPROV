DROP TABLE IF EXISTS public.part CASCADE;
create table public.part
(
    p_partkey   INTEGER     NOT NULL PRIMARY KEY,
    p_name      VARCHAR(55),
    p_mfgr      CHAR(6),
    p_category  CHAR(7)     NOT NULL,
    p_brand1    CHAR(9),
    p_color     VARCHAR(11) NOT NULL,
    p_type      varchar(25),
    p_size      NUMERIC(2, 0),
    p_container CHAR(10),
    prov        VARCHAR(10)
);

DROP TABLE IF EXISTS public.supplier CASCADE;
create table public.supplier
(
    s_suppkey INTEGER     NOT NULL PRIMARY KEY,
    s_name    CHAR(25)    NOT NULL,
    s_address VARCHAR(25) NOT NULL,
    s_city    CHAR(10)    NOT NULL,
    s_nation  CHAR(15)    NOT NULL,
    s_region  CHAR(12)    NOT NULL,
    s_phone   CHAR(15)    NOT NULL,
    prov      VARCHAR(10)
);

DROP TABLE IF EXISTS public.customer CASCADE;
create table public.customer
(
    c_custkey    INTEGER     NOT NULL PRIMARY KEY,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_city       CHAR(10)    NOT NULL,
    c_nation     CHAR(15)    NOT NULL,
    c_region     CHAR(12)    NOT NULL,
    c_phone      CHAR(15)    NOT NULL,
    c_mktsegment CHAR(10)    NOT NULL,
    prov         VARCHAR(10)
);

DROP TABLE IF EXISTS public.date cascade ;
CREATE TABLE date
(
    d_datekey          NUMERIC(8, 0) NOT NULL,
    d_date             CHAR(18)      NOT NULL,
    d_dayofweek        CHAR(9)       NOT NULL, -- defined in Section 2.6 as Size 8, but Wednesday is 9 letters
    d_month            CHAR(9)       NOT NULL,
    d_year             NUMERIC(4, 0) NOT NULL,
    d_yearmonthnum     NUMERIC(6, 0) NOT NULL,
    d_yearmonth        CHAR(7)       NOT NULL,
    d_daynuminweek     NUMERIC(1, 0) NOT NULL,
    d_daynuminmonth    NUMERIC(2, 0) NOT NULL,
    d_daynuminyear     NUMERIC(3, 0) NOT NULL,
    d_monthnuminyear   NUMERIC(2, 0) NOT NULL,
    d_weeknuminyear    NUMERIC(2, 0) NOT NULL,
    d_sellingseason    CHAR(12)      NOT NULL,
    d_lastdayinweekfl  NUMERIC(1, 0) NOT NULL,
    d_lastdayinmonthfl NUMERIC(1, 0) NOT NULL,
    d_holidayfl        NUMERIC(1, 0) NOT NULL,
    d_weekdayfl        NUMERIC(1, 0) NOT NULL,
    prov               VARCHAR(10)
);

ALTER TABLE date
    ADD CONSTRAINT pk_date PRIMARY KEY (d_datekey);

DROP TABLE IF EXISTS public.lineorder cascade;
create table public.lineorder
(
    lo_orderkey      INTEGER               NOT NULL,
    lo_linenumber    INTEGER               NOT NULL,
    lo_custkey       INTEGER               NOT NULL,
    lo_partkey       INTEGER               NOT NULL,
    lo_suppkey       INTEGER               NOT NULL,
    lo_orderdate     NUMERIC(8, 0)         NOT NULL,
    lo_orderpriority CHARACTER VARYING(15) NOT NULL,
    lo_shippriority  CHARACTER(1)          NOT NULL,
    lo_quantity      NUMERIC(2, 0)         NOT NULL,
    lo_extendedprice NUMERIC(15, 0)        NOT NULL,
    lo_ordtotalprice NUMERIC(15, 0)        NOT NULL,
    lo_discount      NUMERIC(2, 0)         NOT NULL,
    lo_revenue       NUMERIC(15, 0)        NOT NULL,
    lo_supplycost    NUMERIC(15, 0)        NOT NULL,
    lo_tax           NUMERIC(1, 0)         NOT NULL,
    lo_commitdate    NUMERIC(8, 0)         NOT NULL,
    lo_shipmode      CHAR(10)              NOT NULL,
    prov             CHARACTER VARYING(10)
);

ALTER TABLE lineorder
    ADD CONSTRAINT pk_lineorder PRIMARY KEY (lo_orderkey, lo_linenumber);

ALTER TABLE lineorder
    ADD CONSTRAINT fk_lineitem_customer FOREIGN KEY (lo_custkey) REFERENCES customer (c_custkey);

ALTER TABLE lineorder
    ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (lo_partkey) REFERENCES part (p_partkey);

ALTER TABLE lineorder
    ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (lo_suppkey) REFERENCES supplier (s_suppkey);

ALTER TABLE lineorder
    ADD CONSTRAINT fk_lineitem_orderdate FOREIGN KEY (lo_orderdate) REFERENCES date (d_datekey);

ALTER TABLE lineorder
    ADD CONSTRAINT fk_lineitem_commitdate FOREIGN KEY (lo_commitdate) REFERENCES date (d_datekey);

CREATE INDEX ssb_d_datekey
    ON date (d_datekey);

CREATE INDEX ssb_d_year
    ON date (d_year);

CREATE INDEX ssb_d_yearmonthnum
    ON date (d_yearmonthnum);

CREATE INDEX ssb_d_weeknuminyear
    ON date (d_weeknuminyear);

CREATE INDEX ssb_p_partkey
    ON part (p_partkey);

CREATE INDEX ssb_p_brand1
    ON part (p_brand1);

CREATE INDEX ssb_p_category
    ON part (p_category);

CREATE INDEX ssb_s_region
    ON supplier (s_region);

CREATE INDEX ssb_s_suppkey
    ON supplier (s_suppkey);

CREATE INDEX ssb_s_nation
    ON supplier (s_nation);

CREATE INDEX ssb_s_city
    ON supplier (s_city);

CREATE INDEX ssb_c_nation
    ON customer (c_nation);

CREATE INDEX ssb_c_custkey
    ON customer (c_custkey);

CREATE INDEX ssb_c_city
    ON customer (c_city);

CREATE INDEX ssb_p_mfgr
    ON part (p_mfgr);

CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION provsql;

SET search_path TO public, provsql;

CREATE TYPE classification_level AS ENUM ('unclassified','restricted','confidential','secret','top_secret');


/* The provenance formula m-semiring */
CREATE TYPE formula_state AS (
  formula text,
  nbargs int
);

CREATE FUNCTION formula_plus_state(state formula_state, value text)
  RETURNS formula_state AS
$$
BEGIN
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' ⊕ ',value),state.nbargs+1);
  END IF;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION formula_times_state(state formula_state, value text)
  RETURNS formula_state AS
$$
BEGIN
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' ⊗ ',value),state.nbargs+1);
  END IF;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION formula_state2formula(state formula_state)
  RETURNS text AS
$$
  SELECT
    CASE
      WHEN state.nbargs<2 THEN state.formula
      ELSE concat('(',state.formula,')')
    END;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE AGGREGATE formula_plus(text)
(
  sfunc = formula_plus_state,
  stype = formula_state,
  initcond = '(𝟘,0)',
  finalfunc = formula_state2formula
);

CREATE AGGREGATE formula_times(text)
(
  sfunc = formula_times_state,
  stype = formula_state,
  initcond = '(𝟙,0)',
  finalfunc = formula_state2formula
);

CREATE FUNCTION formula_monus(formula1 text, formula2 text) RETURNS text AS
$$
  SELECT concat('(',formula1,' ⊖ ',formula2,')')
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION formula(token UUID, token2value regclass)
  RETURNS text AS
$$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    '𝟙'::text,
    'formula_plus',
    'formula_times',
    'formula_monus');
END
$$ LANGUAGE plpgsql;

/* The counting m-semiring */

CREATE FUNCTION counting_plus_state(state INTEGER, value INTEGER)
  RETURNS INTEGER AS
$$
  SELECT CASE WHEN state IS NULL THEN value ELSE state + value END
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION counting_times_state(state INTEGER, value INTEGER)
  RETURNS INTEGER AS
$$
SELECT CASE WHEN state IS NULL THEN value ELSE state * value END
$$ LANGUAGE SQL IMMUTABLE;

CREATE AGGREGATE counting_plus(INTEGER)
(
  sfunc = counting_plus_state,
  stype = INTEGER,
  initcond = 0
);

CREATE AGGREGATE counting_times(INTEGER)
(
  sfunc = counting_times_state,
  stype = INTEGER,
  initcond = 1
);

CREATE FUNCTION counting_monus(counting1 INTEGER, counting2 INTEGER) RETURNS INTEGER AS
$$
  SELECT CASE WHEN counting1 < counting2 THEN 0 ELSE counting1 - counting2 END
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE FUNCTION counting(token UUID, token2value regclass)
  RETURNS INTEGER AS
$$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    1,
    'counting_plus',
    'counting_times',
    'counting_monus');
END
$$ LANGUAGE plpgsql;

/* The security semiring */
CREATE FUNCTION security_min_state(state classification_level, level classification_level)
  RETURNS classification_level AS
$$
  SELECT CASE WHEN state IS NULL THEN level WHEN state<level THEN state ELSE level END
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION security_max_state(state classification_level, level classification_level)
  RETURNS classification_level AS
$$
  SELECT CASE WHEN state IS NULL THEN level WHEN state<level THEN level ELSE state END
$$ LANGUAGE SQL IMMUTABLE;

CREATE AGGREGATE security_min(classification_level)
(
  sfunc = security_min_state,
  stype = classification_level,
  initcond = 'top_secret'
);

CREATE AGGREGATE security_max(classification_level)
(
  sfunc = security_max_state,
  stype = classification_level,
  initcond = 'unclassified'
);

CREATE FUNCTION security(token UUID, token2value regclass)
  RETURNS classification_level AS
$$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    'unclassified'::classification_level,
    'security_min',
    'security_max');
END
$$ LANGUAGE plpgsql;

SELECT add_provenance('customer');

SELECT add_provenance('date');

SELECT add_provenance('lineorder');

SELECT add_provenance('part');

SELECT add_provenance('supplier');

SELECT create_provenance_mapping('prov','customer','prov');

SELECT create_provenance_mapping('_date','date','prov');

SELECT create_provenance_mapping('_lineorder','lineorder','prov');

SELECT create_provenance_mapping('_part','part','prov');

SELECT create_provenance_mapping('_supplier','supplier','prov');

INSERT INTO prov
SELECT *
FROM _date;

INSERT INTO prov
SELECT *
FROM _lineorder;

INSERT INTO prov
SELECT *
FROM _part;

INSERT INTO prov
SELECT *
FROM _supplier;


DO
$$
DECLARE
    rec RECORD;
DECLARE
    iterator float4 := 1;
BEGIN
    FOR rec IN SELECT * FROM lineorder
    LOOP
        UPDATE lineorder
        SET prov = 'LO' || iterator
        WHERE lo_orderkey = rec.lo_orderkey and lo_linenumber = rec.lo_linenumber;

        iterator := iterator + 1;
    END LOOP;
END;
$$;

-- IF NECESSARY
DROP INDEX ssb_d_datekey;

DROP INDEX ssb_d_year;

DROP INDEX ssb_d_yearmonthnum;

DROP INDEX ssb_d_weeknuminyear;

DROP INDEX ssb_p_partkey;

DROP INDEX ssb_p_brand1;

DROP INDEX ssb_p_category;

DROP INDEX ssb_s_region;

DROP INDEX ssb_s_suppkey;

DROP INDEX ssb_s_nation;

DROP INDEX ssb_s_city;

DROP INDEX ssb_c_nation;

DROP INDEX ssb_c_custkey;

DROP INDEX ssb_c_city;

DROP INDEX ssb_p_mfgr;