/*
 * COPYRIGHT. HSBC HOLDINGS PLC 2015. ALL RIGHTS RESERVED.
 * 
 * This software is only to be used for the purpose for which it has been
 * provided. No part of it is to be reproduced, disassembled, transmitted,
 * stored in a retrieval system nor translated in any human or computer
 * language in any way or for any other purposes whatsoever without the prior
 * written consent of HSBC Holdings plc.
 */

package com.hsbc.hbme.connect.archive.business;

import java.io.BufferedWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.hsbc.hbme.connect.archive.db.DbmsOutput;
import com.hsbc.hbme.connect.archive.db.UtilDB;
import com.hsbc.hbme.connect.archive.file.UtilFile;
import com.hsbc.hbme.connect.archive.util.BackupBean;
import com.hsbc.hbme.connect.archive.util.Constants;
import com.hsbc.hbme.connect.archive.util.Util;

/**
 * Backup class.
 */
public final class Backup {

    /**
     * Class logger.
     */
    private static Logger logger = Logger.getLogger(Backup.class);

    /**
     * Private constructor.
     */
    private Backup() {
        super();
    }

    /**
     * Does the archive process.
     * 
     * @param period
     *            Archive period (months).
     * @param country
     *            Country code.
     * @param bean
     *            Backup configuration bean.
     * @throws SQLException
     */
    public static void doArchive(final int period, final String country, final BackupBean bean) throws SQLException {
        // Get today's date.
        final Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        // Open connection
        final Connection conn = UtilDB.openConnection(bean);
        try {
            Backup.logger
                .info(new Date() + " - Processing Archive of " + bean.getMainSchema() + " into " + bean.getArchiveSchema());
            // Create path for the log file.
            final String path = UtilFile.createPath(c, bean.getArchivePath());
            // Create log file name.
            final String logFile = UtilFile.createFileName(c, "ARCHIVE_" + bean.getMainSchema() + "_");
            // Open log file.
            final BufferedWriter buff1 = UtilFile.openFile(path + Constants.PATH_SEPARATOR + logFile);
            UtilFile.writeRow(buff1, new Date() + " - Archive process of Connect Plus");
            UtilFile.writeRow(buff1, "Archive of Schema: " + bean.getMainSchema() + " into: " + bean.getArchiveSchema()
                + " -  Country: " + country + " - Period: " + period);
            // Set the date for archiving.
            c.add(Calendar.MONTH, -period);
            UtilFile.writeRow(buff1, "All data before " + Util.dateFormat(c.getTime(), Constants.DATE_INPUT) + " will be archived");
            UtilFile.writeRow(buff1, "");
            // Execute Archive Procedure.

            callArchive(conn, buff1, period, country, bean);
            printLogTable(conn, buff1,
                "SELECT SYS_RECORDKEY, SYS_CREATIONDATE, T_197_APPLICATION_UNIQUECPLUSA FROM " + bean.getMainSchema()
                    + ".ARCHIVE_LOG");

            UtilFile.writeRow(buff1, new Date() + " - End of process...");
            UtilFile.closeFile(buff1);
            UtilDB.closeConnection(conn);
        } catch (SQLException e) {
            Backup.logger.error(Constants.ERROR_SQL, e);
            throw e;
        } finally {
            if (null != conn) {
                conn.close();
            }
        }

    }

    /**
     * Does the purge process.
     * 
     * @param period
     *            Purge period (months).
     * @param country
     *            Country code.
     * @param bean
     *            Backup configuration bean.
     * @throws SQLException
     */
    public static void doPurge(final int period, final String country, final BackupBean bean) throws SQLException {
        // Get today's date.
        final Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        // Open connection
        final Connection conn = UtilDB.openConnection(bean);
        try {
            if (null != conn) {
                Backup.logger.info(new Date() + " - Processing Purge of " + bean.getMainSchema());
                // Create path for the log file.
                final String path = UtilFile.createPath(c, bean.getPurgePath());
                // Create log file name.
                final String logFile = UtilFile.createFileName(c, "PURGE_" + bean.getMainSchema() + "_");
                // Open log file.
                final BufferedWriter buff1 = UtilFile.openFile(path + Constants.PATH_SEPARATOR + logFile);
                UtilFile.writeRow(buff1, new Date() + " - Purge process of Connect Plus");
                UtilFile.writeRow(buff1, "Purge of Schema: " + bean.getMainSchema() + " -  Country: " + country + " - Period: "
                    + period);
                // Set the date for purging.
                c.add(Calendar.MONTH, -period);
                UtilFile.writeRow(buff1, "All data before " + Util.dateFormat(c.getTime(), Constants.DATE_INPUT)
                    + " will be deleted");
                UtilFile.writeRow(buff1, "");
                // Execute Purge Procedure.

                callPurge(conn, buff1, period, country, bean);
                printLogTable(conn, buff1,
                    "SELECT SYS_RECORDKEY, SYS_CREATIONDATE, T_197_APPLICATION_UNIQUECPLUSA FROM " + bean.getMainSchema()
                        + ".PURGE_LOG");
                UtilFile.writeRow(buff1, new Date() + " - End of process...");
                UtilFile.closeFile(buff1);

            }
        } catch (SQLException e) {
            Backup.logger.error(Constants.ERROR_SQL, e);
        } finally {
            if (null != conn) {
                conn.close();
            }
        }
    }

    /**
     * Call the Archive procedure.
     * 
     * @param conn
     *            Connection.
     * @param buff1
     *            Archive Log file.
     * @param period
     *            Archive period.
     * @param country
     *            Country code.
     * @param bean
     *            Backup configuration bean.
     * @throws SQLException
     *             SQLException.
     */
    private static void callArchive(final Connection conn, final BufferedWriter buff1, final int period, final String country,
        final BackupBean bean) throws SQLException {
        UtilFile.writeRow(buff1, "CALLING " + bean.getMainSchema()
            + ".PKG_ARCHIVE_PURGE_CONNECT_PLUS.ARCHIVE_CPLUS(? ,?, ?, ?, ?)}");
        final String sql = "{CALL " + bean.getMainSchema() + ".PKG_ARCHIVE_PURGE_CONNECT_PLUS.ARCHIVE_CPLUS(? ,?, ?, ?, ?)}";
        UtilFile
            .writeRow(buff1, "CALLED " + bean.getMainSchema() + ".PKG_ARCHIVE_PURGE_CONNECT_PLUS.ARCHIVE_CPLUS(? ,?, ?, ?, ?)}");
        final DbmsOutput dbmsOutput = new DbmsOutput(conn);
        dbmsOutput.enable(Constants.DBMS_OUT_SIZE);
        final CallableStatement stmt = conn.prepareCall(sql);
        stmt.setString(1, bean.getMainSchema());
        stmt.setString(2, bean.getArchiveSchema());
        stmt.setInt(Constants.INT_3, period);
        stmt.setString(Constants.INT_4, country);
        stmt.setString(5, bean.getDbUsr());
        try {
            stmt.execute();
        } catch (SQLException e) {
            Backup.logger.error(Constants.ERROR_SQL, e);
            stmt.close();
            dbmsOutput.save(buff1);
            dbmsOutput.close();
            throw e;
        }
        stmt.close();
        dbmsOutput.save(buff1);
        dbmsOutput.close();
    }

    /**
     * Call the Purge procedure.
     * 
     * @param conn
     *            Connection.
     * @param buff1
     *            Purge Log file.
     * @param period
     *            Purge period.
     * @param country
     *            Country code.
     * @param bean
     *            Backup configuration bean.
     * @throws SQLException
     *             SQLException.
     */
    private static void callPurge(final Connection conn, final BufferedWriter buff1, final int period, final String country,
        final BackupBean bean) throws SQLException {
        UtilFile.writeRow(buff1, "CALLING " + bean.getMainSchema() + ".PKG_ARCHIVE_PURGE_CONNECT_PLUS.PURGE_CPLUS(? ,?, ?)}");
        final String sql = "{CALL " + bean.getMainSchema() + ".PKG_ARCHIVE_PURGE_CONNECT_PLUS.PURGE_CPLUS(? ,?, ?, ?)}";
        UtilFile.writeRow(buff1, "CALLED " + bean.getMainSchema() + ".PKG_ARCHIVE_PURGE_CONNECT_PLUS.PURGE_CPLUS(? ,?, ?)}");
        final DbmsOutput dbmsOutput = new DbmsOutput(conn);
        dbmsOutput.enable(Constants.DBMS_OUT_SIZE);
        final CallableStatement stmt = conn.prepareCall(sql);
        stmt.setString(1, bean.getMainSchema());
        stmt.setInt(2, period);
        stmt.setString(Constants.INT_3, country);
        stmt.setString(4, bean.getDbUsr());
        stmt.execute();
        stmt.close();
        dbmsOutput.save(buff1);
        dbmsOutput.close();
    }

    /**
     * Write the processed rows info into the log file.
     * 
     * @param conn
     *            Connection.
     * @param buff1
     *            Log file.
     * @param query
     *            SQL sentence.
     * @throws SQLException
     *             SQLException.
     */
    private static void printLogTable(final Connection conn, final BufferedWriter buff1, final String query) throws SQLException {
        final PreparedStatement pstmt = UtilDB.doQuery(conn, query);
        final ResultSet rs = UtilDB.getData(pstmt);
        try {
            if (null != rs) {
                UtilFile.writeRow(buff1, "NOTICE!!");
                UtilFile.writeRow(buff1, "The following rows of APPLICANTS and related tables have been affected by the process");
                UtilFile.writeRow(buff1, "Connect Plus ID - Creation date - Record Key");
                while (rs.next()) {
                    UtilFile.writeRow(
                        buff1,
                        rs.getString("T_197_APPLICATION_UNIQUECPLUSA") + " - " + rs.getString("SYS_CREATIONDATE") + " - "
                            + rs.getString("SYS_RECORDKEY"));
                }
                rs.close();
            }
            UtilFile.writeRow(buff1, "");

            pstmt.close();
        } catch (SQLException e) {
            Backup.logger.error(Constants.ERROR_SQL, e);
        } finally {
            if (null != rs) {
                rs.close();
            }
        }
    }

    /**
     * Delete the temporary table which holds the affected rows info.
     * 
     * @param conn
     *            Connection.
     * @param query
     *            SQL statement.
     * @throws SQLException
     *             SQLException.
     */
    private static void deleteLogTable(final Connection conn, final String query) throws SQLException {
        final PreparedStatement pstmt = UtilDB.doQuery(conn, query);
        pstmt.execute();
        pstmt.close();
    }

}
