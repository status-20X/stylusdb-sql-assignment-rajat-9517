// src/queryParser.js

function parseQuery(query) {

    try { 
    // First, let's trim the query to remove any leading/trailing whitespaces
    query = query.trim();

    let isDistinct = false;
    if (query.toUpperCase().includes('SELECT DISTINCT')) {
        isDistinct = true;
        query = query.replace('SELECT DISTINCT', 'SELECT');
    }


    // Initialize variables for different parts of the query
    let selectPart, fromPart;

    // Split the query at the WHERE clause if it exists
    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // Everything before WHERE clause

    // WHERE clause is the second part after splitting, if it exists
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    // Split the remaining query at the JOIN clause if it exists
    const joinSplit = query.split(/\sINNER JOIN\s/i);
    selectPart = joinSplit[0].trim(); // Everything before JOIN clause

    // JOIN clause is the second part after splitting, if it exists
    const joinPart = joinSplit.length > 1 ? joinSplit[1].trim() : null;

    // Parse the SELECT part
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }

    const [, fields, table] = selectMatch;

    //Parse the JOIN part if it exists
    // let joinTable = null, joinCondition = null;
    // if (joinPart) {
    //     const joinRegex = /^(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    //     const joinMatch = joinPart.match(joinRegex);
    //     if (!joinMatch) {
    //         throw new Error('Invalid JOIN format');
    //     }

    //     joinTable = joinMatch[1].trim();
    //     joinCondition = {
    //         left: joinMatch[2].trim(),
    //         right: joinMatch[3].trim()
    //     };
    // }

   

    // Extract JOIN information
    const { joinType, joinTable, joinCondition } = parseJoinClause(query);
    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }

    return {
        fields: fields.split(',').map(field => field.trim()),
        table: table.trim(),
        whereClauses,
        joinCondition,
        joinTable,
        joinType
        
    };

} catch (error) {
        throw new Error(`Query parsing error: ${error.message}`);
    }
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}

module.exports = {parseQuery, parseJoinClause};