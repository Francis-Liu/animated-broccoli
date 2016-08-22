# Copyright (c) 2011 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
The FilterScheduler is for creating instances locally.
You can customize this scheduler by specifying your own Host Filters and
Weighing Functions.
"""

import random

from oslo_log import log as logging
from six.moves import range

import nova.conf
from nova import exception
from nova.i18n import _
from nova import rpc
from nova.scheduler import driver
from nova.scheduler import scheduler_options
import time
import datetime


CONF = nova.conf.CONF
LOG = logging.getLogger(__name__)


class FilterScheduler(driver.Scheduler):
    """Scheduler that can be used for filtering and weighing."""
    def __init__(self, *args, **kwargs):
        super(FilterScheduler, self).__init__(*args, **kwargs)
        self.options = scheduler_options.SchedulerOptions()
        self.notifier = rpc.get_notifier('scheduler')

    def select_destinations(self, context, spec_obj):
        """Selects a filtered set of hosts and nodes."""
        self.notifier.info(
            context, 'scheduler.select_destinations.start',
            dict(request_spec=spec_obj.to_legacy_request_spec_dict()))

        num_instances = spec_obj.num_instances
        selected_hosts = self._schedule(context, spec_obj)

        # Couldn't fulfill the request_spec
        if len(selected_hosts) < num_instances:
            # NOTE(Rui Chen): If multiple creates failed, set the updated time
            # of selected HostState to None so that these HostStates are
            # refreshed according to database in next schedule, and release
            # the resource consumed by instance in the process of selecting
            # host.
            for host in selected_hosts:
                host.obj.updated = None

            # Log the details but don't put those into the reason since
            # we don't want to give away too much information about our
            # actual environment.
            LOG.debug('There are %(hosts)d hosts available but '
                      '%(num_instances)d instances requested to build.',
                      {'hosts': len(selected_hosts),
                       'num_instances': num_instances})

            reason = _('There are not enough hosts available.')
            raise exception.NoValidHost(reason=reason)

        dests = [dict(host=host.obj.host, nodename=host.obj.nodename,
                      limits=host.obj.limits) for host in selected_hosts]

        self.notifier.info(
            context, 'scheduler.select_destinations.end',
            dict(request_spec=spec_obj.to_legacy_request_spec_dict()))
        return dests

    def _get_configuration_options(self):
        """Fetch options dictionary. Broken out for testing."""
        return self.options.get_configuration()

    def _schedule(self, context, spec_obj, more=True):
        """Returns a list of hosts that meet the required specs,
        ordered by their fitness.
        """
        elevated = context.elevated()

        config_options = self._get_configuration_options()

        # Find our local list of acceptable hosts by repeatedly
        # filtering and weighing our options. Each time we choose a
        # host, we virtually consume resources on it so subsequent
        # selections can adjust accordingly.

        # Note: remember, we are using an iterator here. So only
        # traverse this list once. This can bite you if the hosts
        # are being scanned in a filter or weighing function.
        hosts = self._get_all_host_states(elevated)

        max_loops = 2
        selected_hosts = []
        more_host_names = []
        num_instances = spec_obj.num_instances
        # NOTE(sbauza): Adding one field for any out-of-tree need
        spec_obj.config_options = config_options

        loop_count = 0
        while loop_count < max_loops:
            loop_count += 1
            for num in range(num_instances):
                # Filter local hosts based on requirements ...
                print "calling get_filtered_hosts with hosts = %s" % hosts
                filtered_hosts = self.host_manager.get_filtered_hosts(hosts,
                        spec_obj, index=num)
                print "filtered_hosts = %s" % filtered_hosts
                if not filtered_hosts:
                    # Can't get any more locally.
                    if more:
                        more = False # one request does not wait twice
                        hosts, more_host_names = self._get_all_host_states(elevated, more_hosts=1) # TODO multiple instances might request more_hosts>1, and wait W per instance
                        if len(more_host_names) > 0:
                            print "calling get_filtered_hosts with newly acquired hosts"
                            filtered_hosts = self.host_manager.get_filtered_hosts(hosts,
                                    spec_obj, index=num)
                            print "new filtered_hosts = %s" % filtered_hosts
                            if not filtered_hosts:
                                print "ERROR - host lock doesn't work!"
                                self.host_manager.do_unlock_hosts(more_host_names)
                                more_host_names = []
                                break
                        else:
                            print "Cannot receive extra nodes"
                            break
                    else:
                        break

                hosts = filtered_hosts
                print "filtered_hosts = %s" % filtered_hosts

                LOG.debug("Filtered %(hosts)s", {'hosts': hosts})

                weighed_hosts = self.host_manager.get_weighed_hosts(hosts,
                        spec_obj)

                LOG.debug("Weighed %(hosts)s", {'hosts': weighed_hosts})

                scheduler_host_subset_size = max(1,
                                                 CONF.scheduler_host_subset_size)
                if scheduler_host_subset_size < len(weighed_hosts):
                    weighed_hosts = weighed_hosts[0:scheduler_host_subset_size]
                chosen_host = random.choice(weighed_hosts)

                LOG.debug("Selected host: %(host)s", {'host': chosen_host})
                selected_hosts.append(chosen_host)

                # Now consume the resources so the filter/weights
                # will change for the next instance.
                chosen_host.obj.consume_from_request(spec_obj)
                if spec_obj.instance_group is not None:
                    spec_obj.instance_group.hosts.append(chosen_host.obj.host)
                    # hosts has to be not part of the updates when saving
                    spec_obj.instance_group.obj_reset_changes(['hosts'])

                # FIXME For reference, this is the Liberty LCRC code
                # scheduler_host_subset_size = CONF.scheduler_host_subset_size
                # if scheduler_host_subset_size > len(weighed_hosts):
                    # scheduler_host_subset_size = len(weighed_hosts)
                # if scheduler_host_subset_size < 1:
                    # scheduler_host_subset_size = 1

                # chosen_host = random.choice(
                    # weighed_hosts[0:scheduler_host_subset_size])
                # LOG.debug("Selected host: %(host)s", {'host': chosen_host})
                # selected_hosts.append(chosen_host)

                # FIXME Noticed while updating to Mitaka. Is the line below used?
                num_instances -= 1

                # # Now consume the resources so the filter/weights
                # # will change for the next instance.
                # chosen_host.obj.consume_from_instance(instance_properties)
                if len(more_host_names):
                    self.host_manager.do_unlock_hosts(more_host_names)
                    more_host_names = []
                # if update_group_hosts is True:
                    # # NOTE(sbauza): Group details are serialized into a list now
                    # # that they are populated by the conductor, we need to
                    # # deserialize them
                    # if isinstance(filter_properties['group_hosts'], list):
                        # filter_properties['group_hosts'] = set(
                            # filter_properties['group_hosts'])
                    # filter_properties['group_hosts'].add(chosen_host.obj.host)

        # FIXME Add code to ask balancer if all selected_hosts are free to be used by OpenStack
        # FIXME FIXME Is the comment above still relevant?

        return selected_hosts

    def _get_all_host_states(self, context, more_hosts=0):
        """Template method, so a subclass can implement caching."""
        return self.host_manager.get_all_host_states(context,
                                                     more_hosts=more_hosts)
