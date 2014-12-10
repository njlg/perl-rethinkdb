$(function() {
  start();
});

function start() {
  var $top = $('#top');
  $('#content').waypoint(function(dir) {
    if( dir === 'down' ) {
      return $top.addClass('show');
    }
    $top.removeClass('show');
  });

  // keep main navigation item in focus
  var scrolling = false,
    scrollTimeout = null;

  var $nav = $('nav').on('scroll', function() {
      scrolling = true;
      if( scrollTimeout != null ) {
        clearTimeout(scrollTimeout);
      }
      scrollTimeout = setTimeout(function() {
        scrolling = false;
        scrollTimeout = null;
      }, 1000);
  });

  $('#content [id]').waypoint(function(dir) {
    var p = $nav.find('a[href="#' + this.id + '"]').parent();

    // skip if there is no navigation item for this
    if(!p.length) {
      return;
    }

    if( scrolling ) {
      return;
    }

    // assign "active" class to correct element in navigation
    var ps = p.siblings();
    if( ps.hasClass('active') ) {
      ps.removeClass('active');
    }
    else {
      // must be going backwards into another section
      $nav.find('.active').removeClass('active');
      var parent = p;
      while( parent && parent[0].tagName !== 'NAV' ) {
        parent.addClass('active');
        parent = parent.parent();
      }
    }

    p.find('ul > .active').removeClass('active');
    p.addClass('active');

    // scroll to active element in navigation area
    var pt = p.offset().top,
      ph = p.height(),
      nt = $nav.offset().top,
      nh = $nav.height();

    if( pt < nt ) {
      $nav.finish().animate({
        scrollTop: $nav.scrollTop() - nh
      });
    }
    else if( (pt + ph - nt) > nh ) {
      $nav.finish().animate({
        scrollTop: pt - nt + $nav.scrollTop()
      });
    }
  }, { offset: '25%' });
}
