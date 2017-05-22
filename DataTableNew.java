package com.inuajamii.test.datatables;

import org.apache.commons.collections.map.HashedMap;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Edudev on 5/17/2017.
 */
@Transactional(readOnly=true)
@Service("dataTableNewService")
@Scope(value="request", proxyMode= ScopedProxyMode.TARGET_CLASS)
public class DataTableNew implements DataTableNewInterface {

    private String[] columns;
    private final List<RowFormatInterface> _formatChain = new ArrayList<>();
    private final Map<String, Collection> _listParams = new HashMap<>();
    private final Map<String, Object> _params = new HashMap<>();
    private final Map<String, Object> _footerColumns = new LinkedHashMap<>();
    private final Map<String, Object> _orderingColumns = new HashMap<>();
    private final List<String> _whereParams = new ArrayList<>();
    private final List<String> _columnNames = new ArrayList<>();
    private boolean _nativeSQL = false;
    private String _groupBy = "";

    private final StringBuilder _selectParams = new StringBuilder();
    private final StringBuilder _fromParams = new StringBuilder();
    private final StringBuilder _joinParams = new StringBuilder();

    @Autowired
    private HttpServletRequest _request;

    @PersistenceContext
    EntityManager entityManager;


    @Override
    public Object emptyResultSet()
    {
        Map<String,Object>map = new HashedMap();
        int draw = Integer.parseInt(_request.getParameter("draw"));

        map.put("draw",draw);
        return map;
    }

    /**
     * Specify the columns that will appear in the final result set in order to
     * assist this class in building the information needed to render the
     * datatable result map
     *
     * @param   select
     * @return  DataTableNewInterface
     */
    @Override
    public DataTableNewInterface select(String select)
    {
        //get string array from select statement, with (,) as the delimeter
        int result = 0;
        for(String column : select.split(","))
        {
            //replace the <;> used in functions with <,> to make a valid sql statement
            column = column.replace(";",",");

            //get the column used (decide if it has an alias) and set it
            int c;
            c = column.toLowerCase().indexOf(" as ");
            if(c > 0)
                column = column.substring(0,c);

            _columnNames.add(column);

            //append the column to the select statement builder
            if(_selectParams.length() > 0)
                _selectParams.append(", ").append(column);
            else
                _selectParams.append(column);

            //append the column identifier
            _selectParams.append(" AS col_").append(result);

            result ++;

        }

        System.err.println("The select statement is " + _selectParams.toString());
        System.err.println("================================================================================");
        //allow the chaining of the params
        return this;
    }

    @Override
    public DataTableNewInterface from(String from)
    {
        // Set the table stuff
        if (_fromParams.length() > 0)
            _fromParams.append(" ").append(from);
        else _fromParams.append(from);

        System.err.println("The from statement is " + _fromParams.toString());
        System.err.println("================================================================================");


        // Allow the chaining of the params
        return this;
    }

    @Override
    public DataTableNewInterface where(String where)
    {
        _whereParams.add(where);

        // Allow the chaining of the params

        System.err.println("The where statement is " + _whereParams.toString());
        System.err.println("================================================================================");
        return this;
    }

    @Override
    public DataTableNewInterface groupBy(String groupBy)
    {
        _groupBy = groupBy;

        System.err.println("The group by statement is " + _groupBy.toString());
        System.err.println("================================================================================");
        // Allow the chaining of the params
        return this;
    }

    @Override
    public DataTableNewInterface join(String join)
    {
//        // Set the table stuff
//        if (_joinParams.length() > 0)
//            _joinParams.append(" LEFT JOIN ").append(join);
//        else _joinParams.append(join);

        _joinParams.append(" LEFT JOIN ").append(join);

        System.err.println("The join statement is " + _joinParams.toString());
        System.err.println("================================================================================");

        // Allow the chaining of the params
        return this;
    }

    @Override
    public DataTableNewInterface nativeSQL(boolean state) {
        _nativeSQL = true;
        return this;
    }

    /**
     * Set the parameter bound to the parameterised query passed in the
     * conditions
     *
     * @param   key
     * @param   value
     * @return  DataTableInterface
     */
    @Override
    public DataTableNewInterface setParameter(String key, Object value) {
        // Check if the key has been set`11
        _params.put(key, value);

        // Allow the chaining of the params
        return this;
    }


    @Override
    public Object showTable()
    {

        //StringQueryBuilder nativeQueryBuilder = new StringQueryBuilder(true,session,StringQueryBuilder.NATIVE_QUERY);
        Map<String,Object>map = new HashedMap();
        map.put("draw",Integer.parseInt(_request.getParameter("draw")));
        map .put("data",buildResultSet(""));
        map.put("recordsTotal",buildResultSet("total"));
        map.put("recordsFiltered",buildResultSet("filtered-total"));

        return map;
    }

    private List<Object[]>buildResultSet(String setting)
    {

        Session session = entityManager.unwrap(Session.class);

        StringBuilder queryBuilder = buildQuery(setting);

        Query query = _nativeSQL ? session.createSQLQuery(queryBuilder.toString()) : null;

        //specify the limit applied to the result set
        if(!setting.equals("filtered-total") && !setting.equals("total"))
            query = setLimit(queryBuilder,session);

        //Set the parameters needed
        if(!_params.isEmpty())
        {
            for(Map.Entry<String,Object> param : _params.entrySet())
            {
                try
                {
                    /*Skip if the setting is 'total' because the search term isn't being set, coz it's the filter*/
                    if(!setting.equals("total"))
                        query.setParameter(param.getKey(),param.getValue());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

        return query.list();
    }




    private StringBuilder buildQuery(String setting)
    {
        StringBuilder query = new StringBuilder();
        StringBuilder params = new StringBuilder();
        StringBuilder generalBuilder = new StringBuilder();

        //if we are getting only the number of records
        if(setting.equals("filtered-total") || setting.equals("total"))
        {
            String column = "";


            if(!_nativeSQL)
            {
                //hql here
            }
            else
            {
                query.append("SELECT ").append(_selectParams).append(" FROM ").append(_fromParams);
            }
        }

        //generating the statement
        else
            query.append("SELECT ").append(_selectParams).append( " FROM  ").append(_fromParams);

        if( _joinParams.length() > 0 )
            query.append(_joinParams);

        //add the conditions
        if(_whereParams.size() > 0)
        {
            for(String item : _whereParams)
            {
                if(params.length() > 0)
                    params.append(" AND ");

                params.append("(").append(item).append(")");
            }

            query.append(" WHERE ").append(params);
        }

        //ADD THE FILTER HERE
        generalBuilder = setFilter(setting);
        if(generalBuilder.length() > 0)
            query.append((_whereParams.size() > 0) ? " AND " : " WHERE ").append(generalBuilder);

        //Set the group by clause
        if(!_groupBy.isEmpty())
            query.append(" GROUP BY ").append(_groupBy);

        //Add the order that will be applied to the result set
        if (!setting.equals("filtered-total") && !setting.equals("total") && !setting.equals("footer-totals"))
        {
            generalBuilder = setOrder();
            if(generalBuilder != null && generalBuilder.length() > 0)
            {
                query.append(" ORDER BY ").append(generalBuilder);
            }
        }


        //if we're implementing a native query
        if ( _nativeSQL && (setting.equals("filtered-total") || setting.equals("total")) ) {
            generalBuilder = new StringBuilder();
            generalBuilder.append("SELECT COUNT(col_0) FROM (").append(query).append(") count");
            query = generalBuilder;
        }
        System.err.println("The entire query is " + query.toString());
        System.err.println("================================================================================");
        return query;
    }

    /**
     * Create the filter query used to filter the information in the result set
     *
     * @return String
     */
    private StringBuilder setFilter(String setting)
    {
        //columns in which the filter is applied
        StringBuilder searchColumns = new StringBuilder();
        StringBuilder filter = new StringBuilder();

        if(!setting.equals("total"))
        {
            String globalSearch = _request.getParameter("search[value]");

            if(!globalSearch.isEmpty())
            {
                for(int i = 0; i < getNumberOfColumns(); i++)
                {
                    //the searchable parameter returns a string of either true or false
                    if(_request.getParameter("columns[" + i + "][searchable]").equals("true") && !globalSearch.isEmpty())
                    {
                        if(columnIsNotAggregate(_columnNames.get(i)))
                        {
                            if(searchColumns.length() > 0)
                                searchColumns.append(" OR ");

                            searchColumns.append("lower(").append(_columnNames.get(i)).append(")").append(" LIKE lower(:searchTerm)");
                        }
                    }
                }

                _params.put("searchTerm", "%" + globalSearch + "%");
                filter.append("(").append(searchColumns).append(")");
            }
        }



        return filter;

    }

    /**
     * Count the number of columns on the datatable request
     *
     *
     * @return  int
     */
    private int getNumberOfColumns()
    {
        Pattern p = Pattern.compile("columns\\[[0-9]+\\]\\[data\\]");
        @SuppressWarnings("rawtypes")
        Enumeration params = _request.getParameterNames();
        List<String> lstOfParams = new ArrayList<>();
        while(params.hasMoreElements()){
            String paramName = (String)params.nextElement();
            Matcher m = p.matcher(paramName);
            if(m.matches())	{
                lstOfParams.add(paramName);
            }
        }
        return lstOfParams.size();
    }


    /**
     * Omit the aggregate functions as you set the filter
     *
     * @param   column
     * @return  Boolean
     */
    private boolean columnIsNotAggregate(String column) {
        String temp = column.toUpperCase().replace(" ", "");
        return !( temp.startsWith("MIN") || temp.startsWith("MAX(") || temp.startsWith("SUM(") || temp.startsWith("COUNT(") );
    }


    /**
     * Define the limit that will be applied to the result set
     *
     * @param   session
     * @return  Query
     */
    private Query setLimit(StringBuilder queryBuilder, Session session)
    {
        Query query = _nativeSQL ? session.createSQLQuery(queryBuilder.toString()): null;
        int start = Integer.parseInt(_request.getParameter("start"));
        int length = Integer.parseInt(_request.getParameter("length"));

        if(!_request.getParameter("start").isEmpty() && start != -1)
        {
            int offset = start / length;
            query.setFirstResult(offset * length);
            query.setMaxResults(length);
        }

        return query;
    }

    /**
     * Set the aux conditions specified in the request parameters used to limit
     * and order the result set
     *
     * @return String
     */
    private StringBuilder setOrder()
    {
        // If there are no parameters defined
        if (_request.getParameter("order[0][column]").isEmpty())
            return null;

        //build the order
        StringBuilder order = new StringBuilder();

        if(!_orderingColumns.isEmpty())
        {
            for (Map.Entry<String, Object> p : _params.entrySet()) {
                order.append( p.getKey() ).append(" ").append( p.getValue());
                break;
            }
        }
        else
        {
            int sortingColumn = Integer.parseInt(_request.getParameter("order[0][column]"));

            if(_request.getParameter("columns[" + sortingColumn + "][orderable]").equals("true"))
            {
                order.append(_columnNames.get(sortingColumn)).append(" ");
                order.append((_request.getParameter("order[0][dir]")).equals("asc") ? "ASC" : "DESC");
            }
        }

        return order;
    }



}
