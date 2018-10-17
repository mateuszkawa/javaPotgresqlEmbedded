package com.json.test.otjpg;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import com.opentable.db.postgres.embedded.FlywayPreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class SomeDaoTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(FlywayPreparer.forClasspathLocation("db/testing"));

    @Test
    public void testEmbeddedPg()
    {
        try (EmbeddedPostgres pg = EmbeddedPostgres.start();
             Connection c = pg.getPostgresDatabase().getConnection()) {
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        } catch (Exception e) {
            System.out.print(e);
            fail();
        }
    }

    @Test
    public void testPreparedSQL()
    {
        try (Connection c = db.getTestDatabase().getConnection();
             Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM foo");
            rs.next();
            assertEquals("{\"bar\": 1}", rs.getString(1));
            rs.close();
        } catch (Exception e) {
            System.out.print(e);
            fail();
        }
    }

    @Test
    public void testPreparedSQLSpecificJSONQuery()
    {
        try (Connection c = db.getTestDatabase().getConnection();
             Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT test ->> 'bar' FROM foo");
            rs.next();
            assertEquals(1, rs.getInt(1));
            rs.close();
        } catch (Exception e) {
            System.out.print(e);
            fail();
        }
    }

}