frappe.listview_settings['Data Import Custom'] = {
    add_fields: ["status"],  
    get_indicator: function(doc) {
        switch (doc.status) {
            case "Preprocessing":
                return ["Preprocessing", "grey", "status,=,'Preprocessing'"];
            case "Running":
                return ["Running", "blue", "status,=,'Running'"];
            case "Partial Success":
                return ["Partial Success", "yellow", "status,=,'Partial Success'"];
            case "Success":
                return ["Success", "green", "status,=,'Success'"];
            case "Error":
                return ["Error", "red", "status,=,'Error'"];
            case "Stopped":
                return ["Stopped", "red", "status,=,'Stopped'"];
            default:
                return [doc.status, "grey"];
        }
    }
};
