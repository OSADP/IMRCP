let aMenu = [
{"page":"map.html","icon":"fa fa-globe","label":"View Map","access":"imrcp-user;imrcp-admin"},
{"page":"scenarios.html","icon":"fa fa-book","label":"Create Scenario","access":"imrcp-user;imrcp-admin"},
{"page":"viewscenarios.html","icon":"fa fa-eye","label":"View Scenarios","access":"imrcp-user;imrcp-admin"},
{"page":"createreports.html","icon":"fa fa-plus-circle","label":"Create Report","access":"imrcp-user;imrcp-admin"},
{"page":"reports.html","icon":"fa fa-newspaper-o","label":"View Reports","access":"imrcp-user;imrcp-admin"},
{"page":"network.html","icon":"fa fa-code-fork","label":"Manage Roads","access":"imrcp-admin"},
{"page":"useradmin.html","icon":"fa fa-cogs","label":"Manage Users","access":"imrcp-admin"},
{"page":"IMRCP-help.pdf","icon":"fa fa-life-ring","label":"Help","access":"imrcp-user;imrcp-admin"},
{"page":"./","icon":"fa fa-sign-out","label":"Logoff","access":"imrcp-user;imrcp-admin"}
];

let aMenuOld = [
{'page': 'map.html', 'icon': 'fa fa-globe', 'label': 'View Map', 'access': ['imrcp-user', 'imrcp-admin']},
{'page': 'scenarios.html', 'icon': 'fa fa-book', 'label': 'Create Scenario', 'access': ['imrcp-user', 'imrcp-admin']},
{'page': 'viewscenarios.html', 'icon': 'fa fa-eye', 'label': 'View Scenarios', 'access': ['imrcp-user', 'imrcp-admin']},
{'page': 'createreports.html', 'icon': 'fa fa-plus-circle', 'label': 'Create Report', 'access': ['imrcp-user', 'imrcp-admin']},
{'page': 'reports.html', 'icon': 'fa fa-newspaper-o', 'label': 'View Reports', 'access': ['imrcp-user', 'imrcp-admin']},
{'page': 'network.html', 'icon': 'fa fa-code-fork', 'label': 'Manage Roads', 'access': ['imrcp-admin']},
{'page': 'useradmin.html', 'icon': 'fa fa-cogs', 'label': 'Manage Users', 'access': ['imrcp-admin']},
{'page': 'IMRCP-help.pdf', 'icon': 'fa fa-life-ring', 'label': 'Help', 'access': ['imrcp-user']},
{'page': './', 'icon': 'fa fa-sign-out', 'label': 'Logoff', 'access': ['imrcp-user']}
];

function check()
{
	let sToken = sessionStorage.token;
	if (sToken === undefined || sToken.length === 0)
	{
		document.location="/"; // redirect to login
		return;
	}
	else
	{
		let bValidGroup = false;
		for (let sGroup of sessionStorage.groups.split(';').values())
		{
			if (g_oRequirements.groups.indexOf(sGroup) >= 0)
			{
				bValidGroup = true;
				break;
			}
		}
		if (!bValidGroup)
		{
			document.location="/"; // redirect to login
			return;
		}

		$.post("api/auth/check",
		{
			"token": sToken
		},
		function(oData, oStatus)
		{
			sToken = oData.token;
			if (sToken === undefined || sToken.length === 0)
			{
				document.location="/"; // redirect to login
				return;
			}
		});
	}

	$("#uname").text(sessionStorage.uname); // set menu user name
	createMenu();
	$(document).trigger("initPage");
}


function createMenu()
{
	let sThisPage = document.location.pathname.substring(1);
	let oMenuList = $("#navbar");
	let sGroup = sessionStorage.groups;
	let sHtml = "";
	for (let oMenuItem of aMenu.values())
	{
		if (oMenuItem.access.indexOf(sGroup) < 0)
			continue;

		if (sThisPage === oMenuItem.page)
			sHtml += '<li><a class="w3-light-gray" href="#">';
		else
			sHtml += `<li><a href="${oMenuItem.page}">`;

		sHtml += `<i class="${oMenuItem.icon}"></i>&nbsp;&nbsp;${oMenuItem.label}</a></li>`;
	}
	oMenuList.html(sHtml);
}


function createMenuOld()
{
	let sThisPage = document.location.pathname.substring(1);
	let oMenuList = $('<ul class="w3-navbar w3-fhwa-navy" id="navbar"></ul>');
	let aGroups = sessionStorage.groups.split(';');
	for (let oMenuItem of aMenu.values())
	{
		let bValidGroup = false;
		for (let sGroup of aGroups.values())
		{
			if (oMenuItem.access.indexOf(sGroup) >= 0)
			{
				bValidGroup = true;
				break;
			}
		}
		if (!bValidGroup)
		{
			continue;
		}
		let sHtml = '<li>';
		if (sThisPage === oMenuItem.page)
			sHtml += '<a class="w3-light-gray" href="#">';
		else
			sHtml += `<a href="${oMenuItem.page}">`;
		sHtml += `<i class="${oMenuItem.icon}"></i>&nbsp;&nbsp;${oMenuItem.label}</a></li>`;
		oMenuList.append(sHtml);
	}
	let oMenuDiv = $('<div></div>');
	oMenuDiv.append(oMenuList);
	$('body').prepend(oMenuDiv);
}


$(document).ready(check);
