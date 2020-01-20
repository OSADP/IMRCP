
$(document).ready(function ()
{
  var clockOptions = {wedges:
  [
      {fillStyle: 'rgba(127, 127, 255, 1.0)', hourStart: 0, minutesDuration: 180, scale: 1.0},
      {fillStyle: 'rgba(127, 255, 255, 1.0)', hourStart: 2, minutesDuration: 180, scale: 0.85}
  ]};
  drawClock(document.getElementById('currentForecast'), clockOptions);
//  drawClock(document.getElementById('nextForecast'), clockOptions);
  // drawClock(document.getElementById('anotherForecest'), 8.5, 45);
  //
  // var ctx = document.getElementById('myChart').getContext('2d');

  var data = {
    labels: ['2:00', '2:30', '3:00', '3:30', '4:00'],
    datasets: [{
        label: 'Some Trend',
        data: [12, 19, 3, 17, 6],
        borderColor: "rgba(153,255,51,0.6)",
        backgroundColor: "rgba(0,0,0,0)"
      }]
  };


  var barData = {
    labels: ['Italy'],
    datasets: [
      {
        label: '2010 customers #',
        backgroundColor: "rgba(153,255,51,0.6)",
        data: [2]
      },
      {
        label: '2014 customers #',
        backgroundColor: "rgba(255,153,0,0.6)",
        data: [6]
      }
    ],
    options: {
      scales: {
        yAxes: [{
            ticks: {
              beginAtZero: true
            }
          }]
      }
    }

  };

  $('#chart1,#chart2,#chart3').each(function ()
  {
    var canvas = this;

    var ctx = canvas.getContext('2d');

    var myChart = new Chart(ctx, {
      type: 'line',
      data: data,
      options: {
        legend: {
          display: false
        }}
    });
  });

  var clientsChart = new Chart(document.getElementById('chart4').getContext('2d'), {
    type: 'bar',
    data:
            {
              //labels: ['Availability'],
              datasets: [
                {
                  label: 'Uptime',
                  backgroundColor: "rgba(153,255,51,0.6)",
                  data: [95]
                },
                {
                  label: 'Downtime',
                  backgroundColor: "rgba(255,153,0,0.6)",
                  data: [5]
                }
              ]
            },
    options: {
      legend: {
        display: false
      },
      scales: {
        xAxes: [{
            stacked: true
          }],
        yAxes: [{
            stacked: true
          }]
      }
    }});
//  var clientsChart2 = new Chart(document.getElementById('chart5').getContext('2d'), {
//    type: 'bar',
//    data: barData});

  var myChart = new Chart(document.getElementById("chart5"), {
    type: 'bar',
    data: {
      // labels: ["Computational Compression"],
      datasets: [
        {
          label: 'Computation Time',
          backgroundColor: "rgba(255,153,0,0.6)",
          data: [1]
        },
        {
          label: 'Forecast Time',
          backgroundColor: "rgba(153,255,51,0.6)",
          data: [6]
        }
      ]
    },
    options: {
      legend: {
        display: false
      },
      scales: {
        yAxes: [{
            ticks: {
              beginAtZero: true
            }
          }]
      }
    }
  });

  $('.chart-anchor').each(function ()
  {
    this.removeAttribute("style");
  });

});

function drawClock(canvas, options)
{
  var ctx = canvas.getContext("2d");
  ctx.arc(100, 100, 90, 0, 2 * Math.PI);
  ctx.stroke();

  var hourRadians = Math.PI / 6;

  var wedges = options.wedges;
  var wedgeCount = wedges.length;
  for (var i = 0; i < wedgeCount; ++i)
  {
    var wedge = wedges[i];
    ctx.beginPath();
    ctx.fillStyle = wedge.fillStyle;

    var hourStart = wedge.hourStart;
    var minutesDuration = wedge.minutesDuration;

    ctx.moveTo(100, 100);

    ctx.arc(100, 100, 90 * wedge.scale,
            (hourStart - 3) * hourRadians,
            (hourStart - 3 + (minutesDuration / 60)) * hourRadians);
    ctx.fill();
  }


  for (var i = 0; i < 12; ++i)
  {
    var radians = i * hourRadians;
    ctx.beginPath();
    ctx.arc(100, 100, 78, radians, radians);
    ctx.arc(100, 100, 90, radians, radians);
    ctx.stroke();
  }


  ctx.font = "16px Trebuchet";
  ctx.fillStyle = 'rgba(0, 0, 0, 1.0)';

  var x = 93 + 70 * Math.cos(9 * hourRadians);
  var y = 105 + 70 * Math.sin(9 * hourRadians);
  ctx.fillText("12", x, y);

  x = 96 + 70 * Math.cos(10 * hourRadians);
  y = 104 + 70 * Math.sin(10 * hourRadians);
  ctx.fillText("1", x, y);

  x = 97 + 70 * Math.cos(11 * hourRadians);
  y = 105 + 70 * Math.sin(11 * hourRadians);
  ctx.fillText("2", x, y);

  x = 97 + 70 * Math.cos(12 * hourRadians);
  y = 106 + 70 * Math.sin(12 * hourRadians);
  ctx.fillText("3", x, y);

  x = 94 + 70 * Math.cos(13 * hourRadians);
  y = 107 + 70 * Math.sin(13 * hourRadians);
  ctx.fillText("4", x, y);

  x = 97 + 70 * Math.cos(14 * hourRadians);
  y = 103 + 70 * Math.sin(14 * hourRadians);
  ctx.fillText("5", x, y);

  x = 95 + 70 * Math.cos(15 * hourRadians);
  y = 103 + 70 * Math.sin(15 * hourRadians);
  ctx.fillText("6", x, y);

  x = 97 + 70 * Math.cos(16 * hourRadians);
  y = 103 + 70 * Math.sin(16 * hourRadians);
  ctx.fillText("7", x, y);

  x = 97 + 70 * Math.cos(17 * hourRadians);
  y = 106 + 70 * Math.sin(17 * hourRadians);
  ctx.fillText("8", x, y);

  x = 97 + 70 * Math.cos(18 * hourRadians);
  y = 105 + 70 * Math.sin(18 * hourRadians);
  ctx.fillText("9", x, y);

  x = 97 + 70 * Math.cos(19 * hourRadians);
  y = 105 + 70 * Math.sin(19 * hourRadians);
  ctx.fillText("10", x, y);

  x = 95 + 70 * Math.cos(20 * hourRadians);
  y = 106 + 70 * Math.sin(20 * hourRadians);
  ctx.fillText("11", x, y);


  for (var i = 0; i < 60; ++i)
  {
    if (i % 5 === 0)
      continue;
    var radians = i * (Math.PI / 30);
    ctx.beginPath();
    ctx.arc(100, 100, 86, radians, radians);
    ctx.arc(100, 100, 90, radians, radians);
    ctx.stroke();
  }


//
//  ctx.fillStyle = "rgba(0, 0, 255, .2)";
//  ctx.beginPath();
//  ctx.moveTo(100, 100);
//  ctx.arc(100, 100, 90,
//          (hourStart - 3) * hourRadians,
//          (hourStart - 3 + (minutesDuration / 60)) * hourRadians);
}
