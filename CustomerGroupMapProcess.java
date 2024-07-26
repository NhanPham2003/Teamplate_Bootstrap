
package com.stb.process;

import com.ewallet.common.SQLUtility;
import com.lpb.ewallet.sql.ConnectionFactory;
import com.lpb.ewallet.utility.Util;
import com.stb.common.STBConstants;
import com.stb.dto.CustomerGroupDTO;
import com.stb.request.CustomerGroupReq;
import com.stb.response.GetCustomerGroupRes;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class CustomerGroupMapProcess {
    private final Logger logger = Logger.getLogger(CustomerGroupMapProcess.class);

    private final Date sysDate = new Date();
    public GetCustomerGroupRes getCustomerGroup() {
        long start = System.currentTimeMillis();
        PreparedStatement stmt =null;
        ResultSet rs = null;
        Connection conn = null;
        GetCustomerGroupRes response = new GetCustomerGroupRes();
        List<CustomerGroupDTO> list = new ArrayList<CustomerGroupDTO>();
        try {
            conn = ConnectionFactory.getConnection();
            String sql = "SELECT cg.group_id, cg.group_code, cg.group_name, " +
                    "count(case when cgm.status= '1' then cgm.map_id end )" + " AS COUNT " +
                    "FROM customer_group cg " +
                    "LEFT JOIN customer_group_map cgm ON cgm.group_id = cg.group_id " +
                    "WHERE cg.parent_id IS NULL AND cg.status = '1' AND cg.group_category = 'LIMITED' " +
                    "GROUP BY cg.group_id, cg.group_code, cg.group_name";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                CustomerGroupDTO group = new CustomerGroupDTO();
                group.setGroupId(rs.getString("GROUP_ID"));
                group.setGroupName(rs.getString("GROUP_NAME"));
                group.setGroupCode(rs.getString("GROUP_CODE"));
                group.setCount(rs.getInt("count"));
                list.add(group);
            }
            response.setList(list);
            response.setResultCode(STBConstants.SUCCESS);
            response.setResultDesc("success");
            logger.info("// GetCustomerGroup.searchTransaction  - size: " + list.size());
            logger.info("getCustomerGroup " + sql);
        } catch (Exception ex) {
            SQLUtility.rollback(conn);
            ex.printStackTrace();
            logger.error("GetCustomerGroupMap.searchTransaction - ERROR = " + ex.getMessage());
            response.setResultCode(STBConstants.ERROR_CODE);
            response.setResultDesc(ex.getMessage());
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(rs);
            SQLUtility.closeObject(stmt);
            long cost = System.currentTimeMillis() - start;
            logger.info("// getCustomerGroup.searchTransaction - cost = " + cost + " ,status: "
                    + ", ResultCode: " + response.getResultCode() + " , ResultDesc: " + response.getResultDesc());
        }
        return response;
    }
    public GetCustomerGroupRes getCustomerGroupMapByFileName(CustomerGroupReq customerGroupReq) {
        long start = System.currentTimeMillis();
        GetCustomerGroupRes response = new GetCustomerGroupRes();
        List<CustomerGroupDTO> list = new ArrayList<>();

        String baseSql = "SELECT cgm.map_id, mc.cust_id, cgm.group_id, mc.full_name, mc.mobile_phone_1, cgm.action, cgm.maker_id," +
                " cgm.maker_date, cgm.checker_id, cgm.checker_date, cgm.status " +
                "FROM customer_group_map cgm " +
                "JOIN master_customer mc ON cgm.cust_id = mc.cust_id " +
                "JOIN customer_group cg ON cgm.group_id = cg.group_id " +
                "WHERE mc.record_stat = 'O' AND cgm.file_name = ? ";
        List<Object> parameters = new ArrayList<>();
        parameters.add(customerGroupReq.getFileNameImport());

        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        if (customerGroupReq.getStatus() != null && !customerGroupReq.getStatus().isEmpty()) {
            sqlBuilder.append(" AND cgm.status = ?");
            parameters.add(customerGroupReq.getFileNameImport());
        }
        if (customerGroupReq.getBranchCode() != null && !customerGroupReq.getBranchCode().isEmpty()) {
            sqlBuilder.append(" AND cgm.branch_code = ?");
            parameters.add(customerGroupReq.getBranchCode());
        }
        if (customerGroupReq.getGroupId() != null && !customerGroupReq.getGroupId().isEmpty()) {
            sqlBuilder.append(" AND cgm.group_id = ?");
            parameters.add(customerGroupReq.getGroupId());
        }
        String sql = sqlBuilder.toString();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionFactory.getConnection();
            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < parameters.size(); i++) {
                if (i + 1 == 1) {
                    stmt.setString(i + 1, (String) parameters.get(i));
                } else {
                    stmt.setObject(i + 1, parameters.get(i));
                }
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    CustomerGroupDTO group = new CustomerGroupDTO();
                    group.setMapId(rs.getString("map_id"));
                    group.setCustId(rs.getString("cust_id"));
                    group.setGroupId(rs.getString("group_id"));
                    group.setFullName(rs.getString("full_name"));
                    group.setMobilePhone(rs.getString("mobile_phone_1"));
                    group.setAction(rs.getString("action"));
                    group.setMakerId(rs.getString("maker_id"));
                    group.setMakerDate(rs.getString("maker_date"));
                    group.setCheckerId(rs.getString("checker_id"));
                    group.setCheckerDate(rs.getString("checker_date"));
                    group.setStatus(rs.getString("status"));
                    list.add(group);
                }
            }

            response.setList(list);
            response.setResultCode(STBConstants.SUCCESS);
            response.setResultDesc("Success");
            logger.info("// GetCustomerGroupMap.searchTransaction - size: " + list.size());
            logger.info("getCustomerGroupMapByFileName " + sql);
        } catch (Exception ex) {
            logger.error("GetCustomerGroupMap.searchTransaction - ERROR = " + ex.getMessage());
            response.setResultCode(STBConstants.ERROR_CODE);
            response.setResultDesc(ex.getMessage());
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(stmt);
            long cost = System.currentTimeMillis() - start;
            logger.info("// getCustomerGroupMapByFileName - cost = " + cost + " ,status: " +
                    ", ResultCode: " + response.getResultCode() + " , ResultDesc: " + response.getResultDesc());
        }
        return response;
    }
    public GetCustomerGroupRes getCustomerGroupMap(CustomerGroupReq customerGroupReq) {
        long start = System.currentTimeMillis();
        GetCustomerGroupRes response = new GetCustomerGroupRes();
        List<CustomerGroupDTO> list = new ArrayList<>();

        String baseSql = "SELECT cgm.map_id, mc.cust_id, mc.full_name, mc.mobile_phone_1, cgm.maker_date , cgm.action , cgm.status " +
                "FROM customer_group_map cgm " +
                "JOIN master_customer mc ON cgm.cust_id = mc.cust_id " +
                "JOIN customer_group cg ON cgm.group_id = cg.group_id " +
                "WHERE mc.record_stat = 'O' ";

        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        List<Object> parameters = new ArrayList<>();
        if (customerGroupReq.getStatus() != null && !customerGroupReq.getStatus().isEmpty()) {
            sqlBuilder.append(" AND cgm.status = ? ");
            parameters.add(customerGroupReq.getStatus());
        }
        if (customerGroupReq.getGroupId() != null && !customerGroupReq.getGroupId().isEmpty()) {
            sqlBuilder.append("AND cg.group_id = ? ");
            parameters.add(customerGroupReq.getGroupId());
        }
        if (customerGroupReq.getGroupCode() != null && !customerGroupReq.getGroupCode().isEmpty()) {
            sqlBuilder.append("AND cg.group_code = ? ");
            parameters.add(customerGroupReq.getGroupCode());
        }
        if (customerGroupReq.getPhoneNumber() != null && !customerGroupReq.getPhoneNumber().isEmpty()) {
            sqlBuilder.append(" AND UPPER(mc.mobile_phone_1) like '%'|| UPPER(?) || '%'");
            parameters.add(customerGroupReq.getPhoneNumber().trim());
        }
        if (customerGroupReq.getFullName() != null && !customerGroupReq.getFullName().isEmpty()) {
            sqlBuilder.append("AND UPPER(mc.full_name) like '%'|| UPPER(?) || '%'");
            parameters.add(customerGroupReq.getFullName().trim());
        }


        return getGetCustomerGroupRes(start, response, list, sqlBuilder, parameters);
    }
    public GetCustomerGroupRes getAllCustomerGroupMapByGroupID(CustomerGroupReq customerGroupReq) {
        long start = System.currentTimeMillis();
        GetCustomerGroupRes response = new GetCustomerGroupRes();
        List<CustomerGroupDTO> list = new ArrayList<>();

        String baseSql = "SELECT cgm.map_id, mc.cust_id, mc.full_name, mc.mobile_phone_1, cgm.maker_date, cgm.action, cgm.status " +
                "FROM customer_group_map cgm " +
                "JOIN master_customer mc ON cgm.cust_id = mc.cust_id " +
                "JOIN customer_group cg ON cgm.group_id = cg.group_id " +
                "WHERE mc.record_stat = 'O' ";

        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        List<Object> parameters = new ArrayList<>();
        if (customerGroupReq.getGroupId() != null && !customerGroupReq.getGroupId().isEmpty()) {
            sqlBuilder.append(" AND cg.group_id = ? ");
            parameters.add(customerGroupReq.getGroupId());
        }
        return getGetCustomerGroupRes(start, response, list, sqlBuilder, parameters);
    }
    private GetCustomerGroupRes getGetCustomerGroupRes(long start, GetCustomerGroupRes response
            , List<CustomerGroupDTO> list, StringBuilder sqlBuilder, List<Object> parameters) {
        String sql = sqlBuilder.toString();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionFactory.getConnection();
            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    CustomerGroupDTO group = new CustomerGroupDTO();
                    group.setMapId(rs.getString("map_id"));
                    group.setCustId(rs.getString("cust_id"));
                    group.setFullName(rs.getString("full_name"));
                    group.setMobilePhone(rs.getString("mobile_phone_1"));
                    group.setMakerDate(rs.getString("maker_date"));
                    group.setAction(rs.getString("action"));
                    group.setStatus(rs.getString("status"));
                    list.add(group);
                }
            }

            response.setList(list);
            response.setResultCode(STBConstants.SUCCESS);
            response.setResultDesc("success");
            logger.info("// GetCustomerGroupMap.  function get customer map - size: " + list.size());
            logger.info("getGetCustomerGroupRes " + sql);
        } catch (Exception ex) {
            logger.error("GetCustomerGroupMap.searchTransaction - ERROR = " + ex.getMessage());
            response.setResultCode(STBConstants.ERROR_CODE);
            response.setResultDesc(ex.getMessage());
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(stmt);
            long cost = System.currentTimeMillis() - start;
            logger.info("// getCustomerGroupMap.searchTransaction - cost = " + cost + " ,status: " +
                    ", ResultCode: " + response.getResultCode() + " , ResultDesc: " + response.getResultDesc());
        }

        return response;
    }
    public List<Long> getCustomerByPhoneNumber(CustomerGroupReq customerGroupReq) {
        long start = System.currentTimeMillis();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;
        List<Long> customerIds = new ArrayList<>();
        try {
            conn = ConnectionFactory.getConnection();
            String sql = "SELECT mc.cust_id FROM master_customer mc WHERE mc.mobile_phone_1 = ? and mc.record_stat = 'O' ";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, customerGroupReq.getPhoneNumber());
            rs = stmt.executeQuery();
            if (rs.next()) {
                Long customerId = rs.getLong("cust_id");
                customerIds.add(customerId);
                logger.info("ID Customer " + customerId);
            }
            rs.close();
            stmt.close();
        } catch (Exception ex) {
            logger.error("Cannot get Customer  " + ex.getMessage());
        }finally {
            long cost = System.currentTimeMillis() - start;
            logger.info("getCustomerByPhoneNumber - cost = " + cost + " ms");
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(rs);
            SQLUtility.closeObject(stmt);

        }
        return customerIds;
    }
    public List<String> getFileImportCustomerGroup(CustomerGroupReq customerGroupReq) {
        long start = System.currentTimeMillis();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;
        List<String> customerIds = new ArrayList<>();
        try {
            conn = ConnectionFactory.getConnection();
            StringBuilder sql = new StringBuilder("SELECT DISTINCT cgm.file_name FROM customer_group_map cgm " +
                    "WHERE cgm.group_id = ? AND cgm.file_name IS NOT NULL");

            if (customerGroupReq.getBranchCode() != null) {
                sql.append(" AND cgm.branch_code = ?");
            }
            if (customerGroupReq.getStatus() != null) {
                if ("2".equals(customerGroupReq.getStatus())){
                    sql.append(" AND ( cgm.status = ? OR cgm.status = 0 ) ");
                }else{
                    sql.append(" AND cgm.status = ?");
                }
            }

            stmt = conn.prepareStatement(sql.toString());
            stmt.setString(1, customerGroupReq.getGroupId());

            if (customerGroupReq.getBranchCode() != null && !"".equals(customerGroupReq.getBranchCode())) {
                stmt.setString(2, customerGroupReq.getBranchCode());
            }
            if (customerGroupReq.getStatus() != null && !"".equals(customerGroupReq.getStatus())) {
                stmt.setString(3, customerGroupReq.getStatus());
            }

            rs = stmt.executeQuery();
            while (rs.next()) {
                String fileName = rs.getString("file_name");
                customerIds.add(fileName);
            }
            logger.info("getFileImportCustomerGroup " + sql);
        } catch (Exception ex) {
            logger.error("Cannot get file name  " + ex.getMessage());
        } finally {
            long cost = System.currentTimeMillis() - start;
            logger.info("get File name - cost = " + cost + " ms");
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(stmt);
            SQLUtility.closeObject(rs);
        }
        return customerIds;
    }

    public int deleteCustomerByMapID(CustomerGroupReq customerGroupReq) {
        PreparedStatement stmt =null;
        Connection conn = null;
        int rowsDeleted = 0;
        try {
            conn = ConnectionFactory.getConnection();
            String sql = "UPDATE customer_group_map SET status = 0, checker_id ?, checker_date = ?, file_name =null, import_date = null WHERE map_id = ?" ;
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, customerGroupReq.getCheckerId());
            stmt.setString(2, "");
            stmt.setString(3, customerGroupReq.getMapId());
            rowsDeleted = stmt.executeUpdate();
            conn.commit();
            logger.info("deleteCustomerByMapID " + sql);
            logger.info("Number of customers deleted: " + rowsDeleted);
        } catch (Exception ex) {
            logger.error("Cannot delete customers: " + ex.getMessage());
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(stmt);
        }
        return rowsDeleted;
    }
    private int maxMapIDInCustomerGroupMapTable() {
        int maxID = 0;
        PreparedStatement stmt = null ;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = ConnectionFactory.getConnection();
            String sql = "SELECT COALESCE(MAX(map_id), 0) + 1 AS map_id FROM customer_group_map";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            if (rs.next()) {
                maxID = rs.getInt("map_id");
            }
            logger.info("Max ID + 1 in table customer_group_map is : " + maxID);
        } catch (Exception ex) {
            logger.error("Cannot get max id customer_group_map: " + ex.getMessage() + ex);
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(rs);
            SQLUtility.closeObject(stmt);
        }
        return maxID;
    }
    public List<Integer> insertCustomerBatch(List<CustomerGroupReq> customerGroupReqList) {
        Connection con = null;
        PreparedStatement pstmt = null;
        int[] updateCounts;
        List<Integer> result = new ArrayList<>();
        List<Integer> insertedIds = new ArrayList<>();

        try {
            con = ConnectionFactory.getConnection();
            con.setAutoCommit(false);
            int count = maxMapIDInCustomerGroupMapTable();
            if (count == 0) return null;

            String sql = "INSERT INTO customer_group_map (map_id, group_id, cust_id, order_no, maker_id, maker_date" +
                    ", checker_id, checker_date, status, role_id, file_name, import_date, action, branch_code) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pstmt = con.prepareStatement(sql);

            for (CustomerGroupReq req : customerGroupReqList) {
                pstmt.setLong(1, count++);
                pstmt.setString(2, req.getGroupId());
                pstmt.setString(3, req.getCustId());
                pstmt.setString(4, req.getOrderNo());
                pstmt.setString(5, req.getMakerID());
                pstmt.setDate(6, new java.sql.Date(sysDate.getTime()));
                pstmt.setString(7, req.getCheckID());
                pstmt.setDate(8, null);
                pstmt.setString(9, req.getStatus());
                pstmt.setString(10, req.getRoleID());
                pstmt.setString(11, req.getFileNameImport());
                pstmt.setDate(12, new java.sql.Date(sysDate.getTime()));
                pstmt.setString(13, req.getAction());
                pstmt.setString(14, req.getBranchCode());
                pstmt.addBatch();
                insertedIds.add(count - 1);
            }

            updateCounts = pstmt.executeBatch();
            con.commit();
            if (updateCounts != null ){
                result = Arrays.stream(updateCounts)
                        .boxed()
                        .collect(Collectors.toList());
            }
            logger.info("insertCustomerBatch " + sql);
            return result;
        } catch (Exception ex) {
            logger.error("Failed to insert customers: " + ex.getMessage() + ex);
        } finally {
            SQLUtility.closeObject(con);
            SQLUtility.closeObject(pstmt);
        }
        return insertedIds;
    }
    public List<Integer> updateCustomerBatch(List<CustomerGroupReq> customerGroupReqList) {
        Connection con = null;
        PreparedStatement pstmt = null;
        List<Integer> result = new ArrayList<>();
        int[] updateCounts;
        Date sysDate = new Date();
        try {
            con =  ConnectionFactory.getConnection();
            con.setAutoCommit(false);

            String sql = "update customer_group_map set checker_date = ?,checker_id = ?, status = ?" +
                    ", file_name = ?, import_date = ?, action = ?" +
                    " WHERE map_id = ? ";
            pstmt = con.prepareStatement(sql);
            for (CustomerGroupReq req : customerGroupReqList) {
                pstmt.setDate(1, new java.sql.Date(sysDate.getTime()));
                pstmt.setString(2, req.getCheckID());
                pstmt.setString(3, req.getStatus());
                pstmt.setString(4, req.getFileNameImport());
                pstmt.setDate(5, new java.sql.Date(sysDate.getTime()));
                pstmt.setString(6, req.getAction());
                pstmt.setString(7, req.getMapId());
                pstmt.addBatch();
            }
            updateCounts = pstmt.executeBatch();
            con.commit();
            if (updateCounts != null ){
                result = Arrays.stream(updateCounts)
                        .boxed()
                        .collect(Collectors.toList());
            }
            logger.info("updateCustomerBatch " + sql);
            return result;
        } catch (Exception ex) {
            logger.error("Failed to update customers: " + ex.getMessage() + ex);
        } finally {
            SQLUtility.closeObject(con);
            SQLUtility.closeObject(pstmt);
        }
        return null;
    }
    public void updateCustomerBatchByIdGroupAndFileName(CustomerGroupReq req) {
        Connection con = null;
        PreparedStatement pstmt = null;
        Date sysDate = new Date();
        try {
            con =  ConnectionFactory.getConnection();
            con.setAutoCommit(false);

            String sql = "update customer_group_map set checker_date = ?,checker_id = ?, status = ?" +
                    ", import_date = NULL" +
                    " WHERE file_name = ? AND group_id = ? ";
            pstmt = con.prepareStatement(sql);
            pstmt.setDate(1, new java.sql.Date(sysDate.getTime()));
            pstmt.setString(2, req.getCheckID());
            pstmt.setString(3, req.getStatus());
            pstmt.setString(4, req.getFileNameImport());
            pstmt.setString(5, req.getCheckerId());

            pstmt.executeUpdate();
            con.commit();
            logger.info("updateCustomerBatchByIdGroupAndFileName " + sql);
        } catch (Exception ex) {
            logger.error("Failed to update customers: " + ex.getMessage() + ex);
        } finally {
            SQLUtility.closeObject(con);
            SQLUtility.closeObject(pstmt);
        }
    }

    public List<Integer> checkExistCustomer(List<CustomerGroupReq> customerGroupReqList) {
        PreparedStatement stmt = null;
        Connection conn = null;
        List<Integer> result = new ArrayList<>();
        ResultSet rs = null;

        try {
            conn = ConnectionFactory.getConnection();
            conn.setAutoCommit(false);

            String sql = "SELECT map_id FROM customer_group_map WHERE cust_id = ? AND group_id = ? AND status = 1";
            stmt = conn.prepareStatement(sql);
            if (customerGroupReqList != null && !customerGroupReqList.isEmpty()) {
                for (CustomerGroupReq x : customerGroupReqList) {
                    stmt.setString(1, x.getCustId());
                    stmt.setString(2, x.getGroupId());
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        result.add(rs.getInt("map_id"));
                    }
                }
            }
            conn.commit();
            logger.error("checkExistCustomer " + sql);
        } catch (Exception ex) {
            logger.error("Cannot check customers: " + ex.getMessage(), ex);
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(rs);
            SQLUtility.closeObject(stmt);
        }
        return result;
    }
    public List<Integer> deleteCustomerBatch(List<CustomerGroupReq> customerGroupReqList) {
        PreparedStatement updateStmt = null ;
        PreparedStatement deleteStmt = null;
        Connection conn = null;
        List<Integer> result = new ArrayList<>();
        Date sysDate = new Date();
        try {
            conn = ConnectionFactory.getConnection();
            conn.setAutoCommit(false);
            if (customerGroupReqList != null && !customerGroupReqList.isEmpty()) {
                List<Integer> mapId = checkExistCustomer(customerGroupReqList);

                String updateSql = "UPDATE customer_group_map SET checker_date = ?, status = 0, file_name = NULL, import_date = NULL WHERE map_id = ?";
                updateStmt = conn.prepareStatement(updateSql);
                if (mapId != null && !mapId.isEmpty()) {
                    for (Integer x : mapId) {
                        updateStmt.setDate(1, new java.sql.Date(sysDate.getTime()));
                        updateStmt.setInt(2, x);
                        updateStmt.addBatch();
                    }
                    logger.error("update customer batch " + updateSql );
                    updateStmt.executeBatch();
                    conn.commit();
                }

                String deleteSql = "DELETE FROM customer_group_map WHERE map_id = ?";
                deleteStmt = conn.prepareStatement(deleteSql);
                for (CustomerGroupReq x : customerGroupReqList) {
                    deleteStmt.setInt(1, Integer.parseInt(x.getMapId()));
                    deleteStmt.addBatch();
                }
                deleteStmt.executeBatch();

                conn.commit();
                if (mapId != null)result.addAll(mapId);
            }

            logger.info("deleteCustomerBatch completed");
        } catch (Exception ex) {
            logger.error("Cannot update or delete customers: " + ex.getMessage() + ex);
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(deleteStmt);
            SQLUtility.closeObject(updateStmt);
        }
        return result;
    }
    public int updateCustomerGroupMap(CustomerGroupReq customerGroupReq) {
        PreparedStatement stmt = null;
        Connection conn = null;
        int rowsUpdated = 0;
        try {
            conn = ConnectionFactory.getConnection();
            String sql = "UPDATE customer_group_map cgm " +
                    "SET cgm.status = ?, " +
                    "    cgm.maker_date = ?, " +
                    "    cgm.checker_id = ?, " +
                    "    cgm.checker_date = ?, " +
                    "    cgm.role_id = '1' " +
                    "WHERE cgm.cust_id = ?";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, customerGroupReq.getOrderId());
            stmt.setString(2, customerGroupReq.getMakerDate());
            stmt.setString(3, customerGroupReq.getCheckerId());
            stmt.setString(4,customerGroupReq.getCheckerDate());
            stmt.setString(5, customerGroupReq.getCustId());

            rowsUpdated = stmt.executeUpdate();
            conn.commit();
            logger.info("Number of rows updated: " + rowsUpdated);
        } catch (Exception ex) {
            logger.error("Cannot update customer group map: " + ex.getMessage() + ex);
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(stmt);
        }
        return rowsUpdated;
    }
    public List<Integer> deleteCustomerInVIPGroup(List<CustomerGroupReq> customerGroupReqList) {
        List<Integer> deletedCustomers = new ArrayList<>();
        Date sysDate = new Date();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            if (customerGroupReqList == null || customerGroupReqList.isEmpty()) {
                logger.error("customerGroupReqList is null or empty");
                return deletedCustomers;
            }
            String sql = "UPDATE customer_group_map SET STATUS = 0, checker_date = ?, checker_id = ?, import_date = NULL " +
                    "WHERE cust_id = ? AND group_id IN (" +
                    customerGroupReqList.stream().map(req -> "?").collect(Collectors.joining(", ")) + ") AND status = 1";
            conn = ConnectionFactory.getConnection();

            stmt = conn.prepareStatement(sql);
            conn.setAutoCommit(false);

            stmt.setDate(1, new java.sql.Date(sysDate.getTime()));
            stmt.setString(2, customerGroupReqList.get(0).getCheckerId());
            stmt.setString(3, customerGroupReqList.get(0).getCustId());

            for (int i = 0; i < customerGroupReqList.size(); i++) {
                if (customerGroupReqList.get(i) != null) {
                    stmt.setString(i + 4, customerGroupReqList.get(i).getGroupId());
                } else {
                    logger.error("CustomerGroupReq at index " + i + " is null");
                }
            }
            stmt.executeUpdate();
            conn.commit();
            logger.info("deleteCustomerInVIPGroup " + sql);

        } catch (Exception ex) {
            logger.error("deleteCustomerInVIPGroup " + ex);
        } finally {
            SQLUtility.closeObject(conn);
            SQLUtility.closeObject(stmt);
        }
        return deletedCustomers;
    }
}